////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2016 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Dr. Frank Celler
/// @author Achim Brandt
////////////////////////////////////////////////////////////////////////////////

#include "GeneralServer.h"

#include "Basics/ConditionLocker.h"
#include "Basics/MutexLocker.h"
#include "Basics/WorkMonitor.h"
#include "Dispatcher/Dispatcher.h"
#include "Dispatcher/DispatcherFeature.h"
#include "Endpoint/EndpointList.h"
#include "GeneralServer/AsyncJobManager.h"
#include "GeneralServer/GeneralCommTask.h"
#include "GeneralServer/GeneralListenTask.h"
#include "GeneralServer/GeneralServerFeature.h"
#include "GeneralServer/GeneralServerJob.h"
#include "GeneralServer/RestHandler.h"
#include "Logger/Logger.h"
#include "Scheduler/ListenTask.h"
#include "Scheduler/Scheduler.h"
#include "Scheduler/SchedulerFeature.h"

using namespace arangodb;
using namespace arangodb::basics;
using namespace arangodb::rest;

namespace {
class GeneralServerThread : public Thread {
 public:
  GeneralServerThread(GeneralServer* server, boost::asio::io_service* ioService)
      : Thread("GeneralServerThread"), _server(server), _ioService(ioService) {}

  ~GeneralServerThread() { shutdown(); }

  void beginShutdown() {
    Thread::beginShutdown();
    _server->wakeup();
  }

 public:
  void run() {
    int idleTries = 0;
    auto server = _server;

    // iterate until we are shutting down
    while (!isStopping()) {
      ++idleTries;

      for (size_t i = 0; i < GeneralServer::SYSTEM_QUEUE_SIZE; ++i) {
        GeneralServer::Job* job = nullptr;
        size_t active = _server->active();

        while (_server->tryActive() && _server->pop(i, job)) {
          LOG_TOPIC(TRACE, Logger::COMMUNICATION)
              << "queuing next job, currently active " << active;

          _server->incActive();
          idleTries = 0;

          _ioService->dispatch([server, job]() {
            job->_callback(std::move(job->_handler));
            server->decActive();
            server->wakeup();
            delete job;
          });

          active = _server->active();
        }
      }

      // we need to check again if more work has arrived after we have
      // aquired the lock. The lockfree queue and _nrWaiting are accessed
      // using "memory_order_seq_cst", this guarantees that we do not
      // miss a signal.

      if (idleTries >= 2) {
        _server->waitForWork();
      }
    }
  }

 private:
  GeneralServer* _server;
  boost::asio::io_service* _ioService;
};
}

// -----------------------------------------------------------------------------
// --SECTION--                                             static public methods
// -----------------------------------------------------------------------------

int GeneralServer::sendChunk(uint64_t taskId, std::string const& data) {
  auto taskData = std::make_unique<TaskData>();

  taskData->_taskId = taskId;
  taskData->_loop = SchedulerFeature::SCHEDULER->lookupLoopById(taskId);
  taskData->_type = TaskData::TASK_DATA_CHUNK;
  taskData->_data = data;

  SchedulerFeature::SCHEDULER->signalTask(taskData);

  return TRI_ERROR_NO_ERROR;
}

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

GeneralServer::GeneralServer(boost::asio::io_service* ioService)
  : _queueStandard(1024),
    _queueAql(1024),
    _queues{&_queueStandard, &_queueAql},
    _active(0), _ioService(ioService) {
  _queueWatcher = new GeneralServerThread(this, _ioService);
}

GeneralServer::~GeneralServer() { stopListening(); }

// -----------------------------------------------------------------------------
// --SECTION--                                                    public
// methods
// -----------------------------------------------------------------------------

void GeneralServer::setEndpointList(EndpointList const* list) {
  _endpointList = list;
}

void GeneralServer::startListening() {
  for (auto& it : _endpointList->allEndpoints()) {
    LOG(TRACE) << "trying to bind to endpoint '" << it.first
               << "' for requests";

    bool ok = openEndpoint(it.second);

    if (ok) {
      LOG(DEBUG) << "bound to endpoint '" << it.first << "'";
    } else {
      LOG(FATAL) << "failed to bind to endpoint '" << it.first
                 << "'. Please check whether another instance is already "
                    "running using this endpoint and review your endpoints "
                    "configuration.";
      FATAL_ERROR_EXIT();
    }
  }

  _queueWatcher->start();
}

void GeneralServer::stopListening() {
  _queueWatcher->beginShutdown();

  for (auto& task : _listenTasks) {
    SchedulerFeature::SCHEDULER->destroyTask(task);
  }

  _listenTasks.clear();
}

bool GeneralServer::queue(
    WorkItem::uptr<RestHandler> handler,
    std::function<void(WorkItem::uptr<RestHandler>)> callback) {
  size_t queue = handler->queue();
  auto job = new GeneralServer::Job(std::move(handler), callback);

  try {
    _queues[queue]->push(job);
  } catch (...) {
    wakeup();
    delete job;
    return false;
  }

  wakeup();
  return true;
}

void GeneralServer::wakeup() {
  CONDITION_LOCKER(guard, _queueCondition);

  guard.signal();
}

void GeneralServer::waitForWork() {
  static uint64_t n = 0;

  CONDITION_LOCKER(guard, _queueCondition);

  for (size_t i = 0; i < SYSTEM_QUEUE_SIZE; ++i) {
    if (!_queues[i]->empty()) {
      return;
    }
  }

  // wait at most 1000ms
  uint64_t waitTime = (1 + (((++n) >> 3) % 9)) * 100 * 1000;

  guard.wait(waitTime);
}

// -----------------------------------------------------------------------------
// --SECTION--                                                 protected
// methods
// -----------------------------------------------------------------------------

bool GeneralServer::openEndpoint(Endpoint* endpoint) {
  ProtocolType protocolType;

  if (endpoint->transport() == Endpoint::TransportType::HTTP) {
    if (endpoint->encryption() == Endpoint::EncryptionType::SSL) {
      protocolType = ProtocolType::HTTPS;
    } else {
      protocolType = ProtocolType::HTTP;
    }
  } else {
    if (endpoint->encryption() == Endpoint::EncryptionType::SSL) {
      protocolType = ProtocolType::VPPS;
    } else {
      protocolType = ProtocolType::VPP;
    }
  }

  ListenTask* task = new GeneralListenTask(this, endpoint, protocolType);

  // ...................................................................
  // For some reason we have failed in our endeavor to bind to the socket
  // -
  // this effectively terminates the server
  // ...................................................................

  if (!task->isBound()) {
    deleteTask(task);
    return false;
  }

  int res = SchedulerFeature::SCHEDULER->registerTask(task);

  if (res == TRI_ERROR_NO_ERROR) {
    _listenTasks.emplace_back(task);
    return true;
  }

  return false;
}
