////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#ifndef ARANGOD_SCHEDULER_JOB_GUARD_H
#define ARANGOD_SCHEDULER_JOB_GUARD_H 1

#include "Basics/Common.h"

#include "Scheduler/Scheduler.h"

namespace arangodb {
namespace rest {
class Scheduler;
}

class JobGuard {
 public:
  JobGuard(rest::Scheduler* scheduler) : _scheduler(scheduler) {}

  ~JobGuard() { release(); }

 public:
  bool tryDirect() {
    bool res = _scheduler->tryDirectThread();

    LOG_TOPIC(TRACE, Logger::COMMUNICATION)
      << "JobGuard::tryDirect returned " << (res ? "true" : "false");

    if (res) {
      _isDirect = true;
    }

    return res;
  }

  void undirect() {
    LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "JobGuard::undirect()";

    _scheduler->undirectThread();
    _isBlocked = false;
  }

  void block() {
    LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "JobGuard::block()";

    _scheduler->blockThread();
    _isBlocked = true;
  }

  void unblock() {
    LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "JobGuard::unblock()";

    _scheduler->unblockThread();
    _isBlocked = false;
  }

  void release() {
    LOG_TOPIC(TRACE, Logger::COMMUNICATION) << "JobGuard::release()";

    if (_isBlocked) {
      unblock();
    }

    if (_isDirect) {
      undirect();
    }
  }

 private:
  rest::Scheduler* _scheduler;
  bool _isDirect = false;
  bool _isBlocked = false;
};
}

#endif
