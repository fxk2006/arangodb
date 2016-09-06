//////////////////////////////////////////////////////////////////////////////
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
/// @author Achim Brandt
/// @author Dr. Frank Celler
/// @author Jan Christoph Uhde
////////////////////////////////////////////////////////////////////////////////

#include "GeneralCommTask.h"

#include "Basics/HybridLogicalClock.h"
#include "Basics/MutexLocker.h"
#include "Basics/StaticStrings.h"
#include "GeneralServer/GeneralServer.h"
#include "GeneralServer/GeneralServerFeature.h"
#include "GeneralServer/RestHandler.h"
#include "GeneralServer/RestHandlerFactory.h"
#include "Logger/Logger.h"
#include "Meta/conversion.h"
#include "Rest/VppResponse.h"
#include "Scheduler/JobGuard.h"
#include "Scheduler/Scheduler.h"
#include "Scheduler/SchedulerFeature.h"

using namespace arangodb;
using namespace arangodb::basics;
using namespace arangodb::rest;

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

GeneralCommTask::GeneralCommTask(EventLoop2 loop, GeneralServer* server,
                                 TRI_socket_t socket, ConnectionInfo&& info,
                                 double keepAliveTimeout)
    : Task2(loop, "GeneralCommTask"),
      SocketTask2(loop, socket, keepAliveTimeout),
      _server(server) {}

// -----------------------------------------------------------------------------
// --SECTION--                                                 protected methods
// -----------------------------------------------------------------------------

void GeneralCommTask::executeRequest(
    std::unique_ptr<GeneralRequest>&& request,
    std::unique_ptr<GeneralResponse>&& response) {
  // check for an async request (before the handler steals the request)
  bool found = false;
  std::string const& asyncExecution =
      request->header(StaticStrings::Async, found);

  // store the message id for error handling
  uint64_t messageId = 0UL;
  if (request) {
    messageId = request->messageId();
  } else if (response) {
    messageId = response->messageId();
  } else {
    LOG_TOPIC(WARN, Logger::COMMUNICATION)
        << "could not find corresponding request/response";
  }

  // create a handler, this takes ownership of request and response
  WorkItem::uptr<RestHandler> handler(
      GeneralServerFeature::HANDLER_FACTORY->createHandler(
          std::move(request), std::move(response)));

  if (handler == nullptr) {
    LOG(TRACE) << "no handler is known, giving up";
    handleSimpleError(rest::ResponseCode::NOT_FOUND, messageId);
    return;
  }

  EventLoop loop;
  handler->setTaskId(_taskId, loop);

  // asynchronous request
  bool ok = false;

  if (found && (asyncExecution == "true" || asyncExecution == "store")) {
    getAgent(messageId)->requestStatisticsAgentSetAsync();
    uint64_t jobId = 0;

    if (asyncExecution == "store") {
      // persist the responses
      ok = handleRequestAsync(std::move(handler), &jobId);
    } else {
      // don't persist the responses
      ok = handleRequestAsync(std::move(handler));
    }

    if (ok) {
      std::unique_ptr<GeneralResponse> response =
          createResponse(rest::ResponseCode::ACCEPTED, messageId);

      if (jobId > 0) {
        // return the job id we just created
        response->setHeaderNC(StaticStrings::AsyncId, StringUtils::itoa(jobId));
      }

      processResponse(response.get());
      return;
    }
  }

  // synchronous request
  else {
    ok = handleRequest(std::move(handler));
  }

  if (!ok) {
    handleSimpleError(rest::ResponseCode::SERVER_ERROR, messageId);
  }
}

void GeneralCommTask::processResponse(GeneralResponse* response) {
  if (response == nullptr) {
    LOG_TOPIC(WARN, Logger::COMMUNICATION)
        << "processResponse received a nullptr, closing connection";
    closeStream();
  } else {
    addResponse(response);
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                                   private methods
// -----------------------------------------------------------------------------

void GeneralCommTask::signalTask(std::unique_ptr<TaskData> data) {
  // data response
  if (data->_type == TaskData::TASK_DATA_RESPONSE) {
    data->RequestStatisticsAgent::transferTo(
        getAgent(data->_response->messageId()));
    processResponse(data->_response.get());
  }

  // data chunk
  else if (data->_type == TaskData::TASK_DATA_CHUNK) {
    handleChunk(data->_data.c_str(), data->_data.size());
  }

  // do not know, what to do - give up
  else {
    closeStream();
  }
}

bool GeneralCommTask::handleRequest(WorkItem::uptr<RestHandler> handler) {
  JobGuard guard(_loop);

  if (handler->isDirect()) {
    handleRequestDirectly(std::move(handler));
    return true;
  }

  if (guard.isIdle()) {
    handleRequestDirectly(std::move(handler));
    return true;
  }

  bool startThread = handler->needsOwnThread();

  if (startThread) {
    guard.block();
    handleRequestDirectly(std::move(handler));
    return true;
  }

  // ok, we need to queue the request
  auto self = this;

  return _server->queue(std::move(handler), [self](WorkItem::uptr<RestHandler> h) {
    self->handleRequestDirectly(std::move(h));
  });
}

void GeneralCommTask::handleRequestDirectly(WorkItem::uptr<RestHandler> h) {
  JobGuard guard(_loop);
  guard.work();

  HandlerWorkStack work(std::move(h));
  auto handler = work.handler();

  uint64_t messageId = 0UL;
  auto req = handler->request();
  auto res = handler->response();
  if (req) {
    messageId = req->messageId();
  } else if (res) {
    messageId = res->messageId();
  } else {
    LOG_TOPIC(WARN, Logger::COMMUNICATION)
        << "could not find corresponding request/response";
  }

  auto agent = getAgent(messageId);

  agent->transferTo(handler);
  RestHandler::status result = handler->executeFull();
  handler->RequestStatisticsAgent::transferTo(agent);

  switch (result) {
    case RestHandler::status::FAILED:
    case RestHandler::status::DONE: {
      addResponse(handler->response());
      break;
    }

    case RestHandler::status::ASYNC:
      handler->release();
      break;
  }
}

bool GeneralCommTask::handleRequestAsync(WorkItem::uptr<RestHandler> handler,
                                       uint64_t* jobId) {
#if 0
  bool startThread = startThread();

  // extract the coordinator flag
  bool found;
  std::string const& hdrStr =
      handler->request()->header(StaticStrings::Coordinator, found);
  char const* hdr = found ? hdrStr.c_str() : nullptr;

  // execute the handler using the dispatcher
  std::unique_ptr<Job> job =
      std::make_unique<HttpServerJob>(this, handler, true);
  task->RequestStatisticsAgent::transferTo(job.get());

  // register the job with the job manager
  if (jobId != nullptr) {
    GeneralServerFeature::JOB_MANAGER->initAsyncJob(
        static_cast<HttpServerJob*>(job.get()), hdr);
    *jobId = job->jobId();
  }

  // execute the handler using the dispatcher
  int res = DispatcherFeature::DISPATCHER->addJob(job, startThread);

  // could not add job to job queue
  if (res != TRI_ERROR_NO_ERROR) {
    job->requestStatisticsAgentSetExecuteError();
    job->RequestStatisticsAgent::transferTo(task);
    if (res != TRI_ERROR_DISPATCHER_IS_STOPPING) {
      LOG(WARN) << "unable to add job to the job queue: "
                << TRI_errno_string(res);
    } else {
      task->handleSimpleError(GeneralResponse::ResponseCode::SERVICE_UNAVAILABLE);
      return true;
    }
    // todo send info to async work manager?
    return false;
  }

  // job is in queue now
  return res == TRI_ERROR_NO_ERROR;
#endif
}

