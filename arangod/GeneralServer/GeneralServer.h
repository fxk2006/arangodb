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
/// @author Jan Steemann
////////////////////////////////////////////////////////////////////////////////

#ifndef ARANGOD_HTTP_SERVER_HTTP_SERVER_H
#define ARANGOD_HTTP_SERVER_HTTP_SERVER_H 1

#include "Scheduler/TaskManager.h"

#include <boost/lockfree/queue.hpp>

#include "Basics/ConditionVariable.h"
#include "Endpoint/ConnectionInfo.h"
#include "GeneralServer/GeneralDefinitions.h"
#include "GeneralServer/HttpCommTask.h"
#include "GeneralServer/HttpsCommTask.h"
#include "GeneralServer/RestHandler.h"

namespace arangodb {
class EndpointList;

namespace rest {

class AsyncJobManager;
class Dispatcher;
class GeneralCommTask;
class HttpServerJob;
class Job;
class ListenTask;
class RestHandlerFactory;

class GeneralServer : protected TaskManager {
  GeneralServer(GeneralServer const&) = delete;
  GeneralServer const& operator=(GeneralServer const&) = delete;

 public:
  class Job {
   public:
    Job(WorkItem::uptr<RestHandler> handler,
        std::function<void(WorkItem::uptr<RestHandler>)> callback)
        : _handler(std::move(handler)), _callback(callback) {}

   public:
    WorkItem::uptr<RestHandler> _handler;
    std::function<void(WorkItem::uptr<RestHandler>)> _callback;
  };

 public:
  // ordered by priority (highst prio first)
  static size_t const AQL_QUEUE = 0;
  static size_t const STANDARD_QUEUE = 1;
  static size_t const SYSTEM_QUEUE_SIZE = 2;

 public:
  static int sendChunk(uint64_t, std::string const&);

 public:
  GeneralServer(size_t queueSize, boost::asio::io_service* ioService);
  virtual ~GeneralServer();

 public:
  void setEndpointList(const EndpointList* list);
  void startListening();
  void stopListening();

  bool queue(WorkItem::uptr<RestHandler>,
             std::function<void(WorkItem::uptr<RestHandler>)>);

  int64_t queueSize(size_t i) { return _queuesSize[i]; }

  bool pop(size_t i, Job*& job) {
    bool ok = _queues[i]->pop(job);

    if (ok && job != nullptr) {
      --(_queuesSize[i]);
    }

    return ok;
  }

  void wakeup();
  void waitForWork();

  size_t active() const { return _active.load(); }
  bool tryActive();
  void releaseActive();

 protected:
  bool openEndpoint(Endpoint* endpoint);

 protected:
  std::vector<ListenTask*> _listenTasks;
  EndpointList const* _endpointList = nullptr;

  boost::lockfree::queue<Job*> _queueStandard;
  boost::lockfree::queue<Job*> _queueAql;
  boost::lockfree::queue<Job*>* _queues[SYSTEM_QUEUE_SIZE];
  std::atomic<int64_t> _queuesSize[SYSTEM_QUEUE_SIZE];
  std::atomic<size_t> _active;

 private:
  basics::ConditionVariable _queueCondition;
  boost::asio::io_service* _ioService;
  Thread* _queueWatcher = nullptr;
};
}
}

#endif
