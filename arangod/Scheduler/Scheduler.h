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

#ifndef ARANGOD_SCHEDULER_SCHEDULER_H
#define ARANGOD_SCHEDULER_SCHEDULER_H 1

#include "Basics/Common.h"

#include "Basics/socket-utils.h"
#include "Logger/Logger.h"
#include "Scheduler/TaskManager.h"

#include "Basics/Mutex.h"

namespace arangodb {
namespace basics {
class ConditionVariable;
}
namespace velocypack {
class Builder;
}

namespace rest {
class SchedulerThread;
class TaskData;

////////////////////////////////////////////////////////////////////////////////
/// @brief input-output scheduler
////////////////////////////////////////////////////////////////////////////////

class Scheduler : private TaskManager {
  Scheduler(Scheduler const&) = delete;
  Scheduler& operator=(Scheduler const&) = delete;

 public:
  //////////////////////////////////////////////////////////////////////////////
  /// @brief constructor
  ///
  /// If the number of threads is one, then the scheduler is single-threaded.
  /// In this case the only methods, which can be called from a different thread
  /// are beginShutdown, isShutdownInProgress, and isRunning. The method
  /// registerTask must be called before the Scheduler is started or from
  /// within the Scheduler thread.
  ///
  /// If the number of threads is greater than one, then the scheduler is
  /// multi-threaded. In this case the method registerTask can be called from
  /// threads other than the scheduler.
  //////////////////////////////////////////////////////////////////////////////

  explicit Scheduler(size_t nrThreads);

  virtual ~Scheduler();

 public:
  boost::asio::io_service* ioService() const { return _ioService; }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief starts scheduler, keeps running
  ///
  /// The functions returns true, if the scheduler has been started. In this
  /// case the condition variable is signal as soon as at least one of the
  /// scheduler threads stops.
  //////////////////////////////////////////////////////////////////////////////

  bool start(basics::ConditionVariable*);

  //////////////////////////////////////////////////////////////////////////////
  /// @brief checks if the scheduler threads are up and running
  //////////////////////////////////////////////////////////////////////////////

  bool isStarted();

  //////////////////////////////////////////////////////////////////////////////
  /// @brief checks if scheduler is still running
  //////////////////////////////////////////////////////////////////////////////

  bool isRunning();

  //////////////////////////////////////////////////////////////////////////////
  /// @brief starts shutdown sequence
  //////////////////////////////////////////////////////////////////////////////

  void beginShutdown();

  //////////////////////////////////////////////////////////////////////////////
  /// @brief checks if scheduler is shuting down
  //////////////////////////////////////////////////////////////////////////////

  bool isShutdownInProgress();

  //////////////////////////////////////////////////////////////////////////////
  /// @brief shuts down the scheduler
  //////////////////////////////////////////////////////////////////////////////

  void shutdown();

  //////////////////////////////////////////////////////////////////////////////
  /// @brief get all user tasks
  //////////////////////////////////////////////////////////////////////////////

  std::shared_ptr<arangodb::velocypack::Builder> getUserTasks();

  //////////////////////////////////////////////////////////////////////////////
  /// @brief get a single user task
  //////////////////////////////////////////////////////////////////////////////

  std::shared_ptr<arangodb::velocypack::Builder> getUserTask(
      std::string const&);

  //////////////////////////////////////////////////////////////////////////////
  /// @brief unregister and delete a user task by id
  //////////////////////////////////////////////////////////////////////////////

  int unregisterUserTask(std::string const&);

  //////////////////////////////////////////////////////////////////////////////
  /// @brief unregister all user tasks
  //////////////////////////////////////////////////////////////////////////////

  int unregisterUserTasks();

  //////////////////////////////////////////////////////////////////////////////
  /// @brief registers a new task
  //////////////////////////////////////////////////////////////////////////////

  int registerTask(Task*);

  //////////////////////////////////////////////////////////////////////////////
  /// @brief registers a new task and returns the chosen threads number
  //////////////////////////////////////////////////////////////////////////////

  int registerTask(Task*, ssize_t* n);

  //////////////////////////////////////////////////////////////////////////////
  /// @brief unregisters a task
  ///
  /// Note that this method is called by the task itself when cleanupTask is
  /// executed. If a Task failed in setupTask, it must not call unregisterTask.
  //////////////////////////////////////////////////////////////////////////////

  int unregisterTask(Task*);

  //////////////////////////////////////////////////////////////////////////////
  /// @brief destroys task
  ///
  /// Even if a Task failed in setupTask, it can still call destroyTask. The
  /// methods will delete the task.
  //////////////////////////////////////////////////////////////////////////////

  int destroyTask(Task*);

  //////////////////////////////////////////////////////////////////////////////
  /// @brief called to display current status
  //////////////////////////////////////////////////////////////////////////////

  void reportStatus();

  //////////////////////////////////////////////////////////////////////////////
  /// @brief whether or not the scheduler is active
  //////////////////////////////////////////////////////////////////////////////

  bool isActive() const { return (bool)_active; }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief set the scheduler activity
  //////////////////////////////////////////////////////////////////////////////

  void setActive(bool value) { _active = value ? 1 : 0; }

  //////////////////////////////////////////////////////////////////////////////
  /// @brief sets the process affinity
  //////////////////////////////////////////////////////////////////////////////

  void setProcessorAffinity(std::vector<size_t> const& cores);

  //////////////////////////////////////////////////////////////////////////////
  /// @brief returns the task for a task id
  ///
  /// Warning: ONLY call this from within the scheduler task! Otherwise, the
  /// task MIGHT already be deleted.
  //////////////////////////////////////////////////////////////////////////////

  Task* lookupTaskById(uint64_t);

  //////////////////////////////////////////////////////////////////////////////
  /// @brief returns the loop for a task id
  //////////////////////////////////////////////////////////////////////////////

  EventLoop lookupLoopById(uint64_t);

 public:
  //////////////////////////////////////////////////////////////////////////////
  /// @brief main event loop
  //////////////////////////////////////////////////////////////////////////////

  virtual void eventLoop(EventLoop) = 0;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief wakes up an event loop
  //////////////////////////////////////////////////////////////////////////////

  virtual void wakeupLoop(EventLoop) = 0;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief called to register a socket descriptor event
  //////////////////////////////////////////////////////////////////////////////

  virtual EventToken installSocketEvent(EventLoop, EventType, Task*,
                                        TRI_socket_t) = 0;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief re-starts the socket events
  //////////////////////////////////////////////////////////////////////////////

  virtual void startSocketEvents(EventToken) = 0;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief stops the socket events
  //////////////////////////////////////////////////////////////////////////////

  virtual void stopSocketEvents(EventToken) = 0;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief called to register a timer event
  //////////////////////////////////////////////////////////////////////////////

  virtual EventToken installTimerEvent(EventLoop, Task*, double timeout) = 0;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief clears a timer without removing it
  //////////////////////////////////////////////////////////////////////////////

  virtual void clearTimer(EventToken) = 0;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief rearms a timer
  //////////////////////////////////////////////////////////////////////////////

  virtual void rearmTimer(EventToken, double timeout) = 0;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief called to register a periodic event
  //////////////////////////////////////////////////////////////////////////////

  virtual EventToken installPeriodicEvent(EventLoop, Task*, double offset,
                                          double interval) = 0;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief rearms a periodic timer
  //////////////////////////////////////////////////////////////////////////////

  virtual void rearmPeriodic(EventToken, double offset, double timeout) = 0;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief called to register a signal event
  //////////////////////////////////////////////////////////////////////////////

  virtual EventToken installSignalEvent(EventLoop, Task*, int signal) = 0;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief called to unregister an event handler
  //////////////////////////////////////////////////////////////////////////////

  virtual void uninstallEvent(EventToken) = 0;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief sends data to a task
  //////////////////////////////////////////////////////////////////////////////

  virtual void signalTask(std::unique_ptr<TaskData>&) = 0;

  void signalTask2(std::unique_ptr<TaskData>);

 private:
  //////////////////////////////////////////////////////////////////////////////
  /// @brief registers a new task
  //////////////////////////////////////////////////////////////////////////////

  int registerTask(Task* task, ssize_t* got, ssize_t want);

  //////////////////////////////////////////////////////////////////////////////
  /// @brief check whether a task can be inserted
  ///
  /// the caller must ensure the schedulerLock is held
  //////////////////////////////////////////////////////////////////////////////

  int checkInsertTask(Task const*);

 protected:
  //////////////////////////////////////////////////////////////////////////////
  /// @brief number of scheduler threads
  //////////////////////////////////////////////////////////////////////////////

  size_t nrThreads;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief scheduler threads
  //////////////////////////////////////////////////////////////////////////////

  SchedulerThread** threads;

 private:
  //////////////////////////////////////////////////////////////////////////////
  /// @brief initializes the signal handlers for the scheduler
  //////////////////////////////////////////////////////////////////////////////

  static void initializeSignalHandlers();

 private:
  //////////////////////////////////////////////////////////////////////////////
  /// @brief true if scheduler is shutting down
  //////////////////////////////////////////////////////////////////////////////

  volatile sig_atomic_t stopping;  // TODO(fc) XXX make this atomic

  //////////////////////////////////////////////////////////////////////////////
  /// @brief true if scheduler is multi-threaded
  //////////////////////////////////////////////////////////////////////////////

  bool multiThreading;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief round-robin for event loops
  //////////////////////////////////////////////////////////////////////////////

  size_t nextLoop;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief lock for scheduler threads
  //////////////////////////////////////////////////////////////////////////////

  Mutex schedulerLock;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief tasks to thread
  //////////////////////////////////////////////////////////////////////////////

  std::unordered_map<Task*, SchedulerThread*> task2thread;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief active tasks
  //////////////////////////////////////////////////////////////////////////////

  std::unordered_map<uint64_t, Task*> taskRegistered;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief scheduler activity flag
  //////////////////////////////////////////////////////////////////////////////

  bool _active = true;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief cores to use for affinity
  //////////////////////////////////////////////////////////////////////////////

  std::vector<size_t> _affinityCores;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief next affinity core to use
  //////////////////////////////////////////////////////////////////////////////

  size_t _affinityPos = 0;

  boost::asio::io_service* _ioService;

 public:
  bool isIdle() {
    if (_nrWorking < _nrRunning) {
      return true;
    }

    if (_nrWorking < _nrRunning + _nrBlocked) {
      startNewThread();
      return true;
    }

    return false;
  }

  void enterThread() {
    int64_t n = ++_nrBusy;

    if (n >= _nrRunning && _nrRunning <= _nrMaximal + _nrBlocked) {
      LOG(ERR) << "STARTING NEW THREAD BUSY " << n;
      startNewThread();
    }
  }

  void unenterThread() { --_nrBusy; }

  void workThread() { ++_nrWorking; }

  void unworkThread() { --_nrWorking; }

  void blockThread() {
    int64_t n = ++_nrBlocked;

    if (_nrRunning <= _nrMaximal + _nrBlocked) {
      LOG(ERR) << "BLOCKED " << n;
      startNewThread();
    }
  }

  void unblockThread() { --_nrBlocked; }

  uint64_t incRunning() { return ++_nrRunning; }

  uint64_t decRunning() { return --_nrRunning; }

  std::string infoStatus() {
    return "busy: " + std::to_string(_nrBusy)
      + ", working: " + std::to_string(_nrWorking)
      + ", blocked: " + std::to_string(_nrBlocked)
      + ", running: " + std::to_string(_nrRunning)
      + ", maximal: " + std::to_string(_nrMaximal);
  }

 private:
  void startNewThread();

  std::atomic<int64_t> _nrBusy;
  std::atomic<int64_t> _nrWorking;
  std::atomic<int64_t> _nrBlocked;
  std::atomic<int64_t> _nrRunning;
  std::atomic<int64_t> _nrMaximal;

  boost::shared_ptr<boost::asio::io_service::work> _workGuard;
};
}
}

#endif
