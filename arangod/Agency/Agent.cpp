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
/// @author Kaveh Vahedipour
////////////////////////////////////////////////////////////////////////////////

#include "Agent.h"
#include "GossipCallback.h"

#include "Basics/ConditionLocker.h"
#include "RestServer/DatabaseFeature.h"
#include "RestServer/QueryRegistryFeature.h"
#include "VocBase/vocbase.h"

#include <velocypack/Iterator.h>
#include <velocypack/velocypack-aliases.h>

#include <chrono>

using namespace arangodb::application_features;
using namespace arangodb::velocypack;

namespace arangodb {
namespace consensus {


/// Agent configuration
Agent::Agent(config_t const& config)
    : Thread("Agent"),
      _config(config),
      _lastCommitIndex(0),
      _spearhead(this),
      _readDB(this),
      _serveActiveAgent(false),
      _nextCompationAfter(_config.compactionStepSize()),
      _inception(std::make_unique<Inception>(this)) {
  _state.configure(this);
  _constituent.configure(this);
}


/// This agent's id
std::string Agent::id() const {
  return config().id();
}


/// Agent's id is set once from state machine
bool Agent::id(std::string const& id) {
  bool success;
  if ((success = _config.setId(id))) {
    LOG_TOPIC(DEBUG, Logger::AGENCY) << "My id is " << id;
  } else {
    LOG_TOPIC(ERR, Logger::AGENCY)
      << "Cannot reassign id once set: My id is " << _config.id()
      << " reassignment to " << id;
  }
  return success;
}


/// Merge command line and persisted comfigurations
bool Agent::mergeConfiguration(VPackSlice const& persisted) {
  return _config.merge(persisted);
}


/// Dtor shuts down thread
Agent::~Agent() {
  shutdown();
}


/// State machine
State const& Agent::state() const {
  return _state;
}


/// Start all agent thread
bool Agent::start() {
  LOG_TOPIC(DEBUG, Logger::AGENCY) << "Starting agency comm worker.";
  Thread::start();
  return true;
}


/// This agent's term
term_t Agent::term() const {
  return _constituent.term();
}


/// Agency size
size_t Agent::size() const {
  return _config.size();
}


/// My endpoint
std::string Agent::endpoint() const {
  return _config.endpoint();
}


/// Handle voting
priv_rpc_ret_t Agent::requestVote(
  term_t t, std::string const& id, index_t lastLogIndex,
  index_t lastLogTerm, query_t const& query) {
  return priv_rpc_ret_t(
    _constituent.vote(t, id, lastLogIndex, lastLogTerm), this->term());
}


/// Get copy of momentary configuration
config_t const Agent::config() const {
  return _config;
}


/// Leader's id
std::string Agent::leaderID() const {
  return _constituent.leaderID();
}


/// Are we leading?
bool Agent::leading() const {
  return _constituent.leading();
}


void Agent::startConstituent() {

  activateAgency();

  auto database = ApplicationServer::getFeature<DatabaseFeature>("Database");
  auto vocbase = database->systemDatabase();
  auto queryRegistry = QueryRegistryFeature::QUERY_REGISTRY;
  _constituent.start(vocbase, queryRegistry);
  
}


// Waits here for confirmation of log's commits up to index.
// Timeout in seconds
bool Agent::waitFor(index_t index, double timeout) {

  if (size() == 1) {  // single host agency
    return true;
  }

  CONDITION_LOCKER(guard, _waitForCV);

  // Wait until woken up through AgentCallback
  while (true) {

    /// success?
    if (_lastCommitIndex >= index) {
      return true;
    }

    // timeout
    if (!_waitForCV.wait(static_cast<uint64_t>(1.0e6 * timeout))) {
      return false;
    }

    // shutting down
    if (this->isStopping()) {
      return false;
    }
  }

  // We should never get here
  TRI_ASSERT(false);

}


//  AgentCallback reports id of follower and its highest processed index
void Agent::reportIn(std::string const& id, index_t index) {

  MUTEX_LOCKER(mutexLocker, _ioLock);

  _lastAcked[id] = std::chrono::system_clock::now();

  if (index > _confirmed[id]) {  // progress this follower?
    _confirmed[id] = index;
  }

  if (index > _lastCommitIndex) {  // progress last commit?

    size_t n = 0;

    for (auto const& i : _config.active()) {
      n += (_confirmed[i] >= index);
    }

    // catch up read database and commit index
    if (n > size() / 2) {

      LOG_TOPIC(DEBUG, Logger::AGENCY) << "Critical mass for commiting "
                                       << _lastCommitIndex + 1 << " through "
                                       << index << " to read db";

      _readDB.apply(_state.slices(_lastCommitIndex + 1, index));
      _lastCommitIndex = index;
      
      if (_lastCommitIndex >= _nextCompationAfter) {
        _state.compact(_lastCommitIndex);
        _nextCompationAfter += _config.compactionStepSize();
      }
      
    }
    
  }

  {
    CONDITION_LOCKER(guard, _waitForCV);
    guard.broadcast();
  }

}


/// Followers' append entries
bool Agent::recvAppendEntriesRPC(
  term_t term, std::string const& leaderId, index_t prevIndex, term_t prevTerm,
  index_t leaderCommitIndex, query_t const& queries) {

  // Update commit index
  if (queries->slice().type() != VPackValueType::Array) {
    LOG_TOPIC(WARN, Logger::AGENCY)
        << "Received malformed entries for appending. Discarting!";
    return false;
  }

  MUTEX_LOCKER(mutexLocker, _ioLock);

  if (this->term() > term) { // peer at higher term 
    if (leaderCommitIndex >= _lastCommitIndex) { // 
      _constituent.follow(term);
    } else {
      LOG_TOPIC(WARN, Logger::AGENCY) << "I have a higher term than RPC caller.";
      return false;
    }
  }
  
  if (!_constituent.vote(term, leaderId, prevIndex, prevTerm, true)) {
    LOG_TOPIC(WARN, Logger::AGENCY) << "Not voting for " << leaderId;
    return false;
  }
  
  size_t nqs   = queries->slice().length();

  if (nqs > 0) {

    size_t ndups = _state.removeConflicts(queries);
    
    if (nqs > ndups) {
      
      LOG_TOPIC(DEBUG, Logger::AGENCY)
        << "Appending " << nqs - ndups << " entries to state machine." <<
        nqs << " " << ndups;

      _state.log(queries, ndups);
  
    }
    
  }
  
  _spearhead.apply(_state.slices(_lastCommitIndex + 1, leaderCommitIndex));
  _readDB.apply(_state.slices(_lastCommitIndex + 1, leaderCommitIndex));
  _lastCommitIndex = leaderCommitIndex;
  
  if (_lastCommitIndex >= _nextCompationAfter) {
    _state.compact(_lastCommitIndex);
    _nextCompationAfter += _config.compactionStepSize();
  }
  
  return true;

}


/// Leader's append entries
priv_rpc_ret_t Agent::sendAppendEntriesRPC(std::string const& follower_id) {
  
  term_t t(0);
  {
    MUTEX_LOCKER(mutexLocker, _ioLock);
    t = this->term();
  }

  index_t last_confirmed = _confirmed[follower_id];
  std::vector<log_t> unconfirmed = _state.get(last_confirmed);
  index_t highest = unconfirmed.back().index;

  if (highest == _lastHighest[follower_id] && (long)(500.0e6*_config.minPing()) >
      (std::chrono::system_clock::now() - _lastSent[follower_id]).count()) {
    return priv_rpc_ret_t(true, t);
  }
  
  // RPC path
  std::stringstream path;
  path << "/_api/agency_priv/appendEntries?term=" << t << "&leaderId=" << id()
       << "&prevLogIndex=" << unconfirmed.front().index
       << "&prevLogTerm=" << unconfirmed.front().term
       << "&leaderCommit=" << _lastCommitIndex;

  // Body
  Builder builder;
  builder.add(VPackValue(VPackValueType::Array));
  for (size_t i = 1; i < unconfirmed.size(); ++i) {
    auto const& entry = unconfirmed.at(i);
    builder.add(VPackValue(VPackValueType::Object));
    builder.add("index", VPackValue(entry.index));
    builder.add("term", VPackValue(entry.term));
    builder.add("query", VPackSlice(entry.entry->data()));
    builder.close();
    highest = entry.index;
  }
  builder.close();

  // Verbose output
 if (unconfirmed.size() > 1) {
    LOG_TOPIC(DEBUG, Logger::AGENCY) << "Appending " << unconfirmed.size() - 1
                                     << " entries up to index " << highest
                                     << " to follower " << follower_id;
  }

  // Send request
  auto headerFields =
    std::make_unique<std::unordered_map<std::string, std::string>>();
  arangodb::ClusterComm::instance()->asyncRequest(
    "1", 1, _config.poolAt(follower_id),
    arangodb::rest::RequestType::POST, path.str(),
    std::make_shared<std::string>(builder.toJson()), headerFields,
    std::make_shared<AgentCallback>(this, follower_id, highest),
    0.1*_config.minPing(), true, 0.05*_config.minPing());
  
  _lastSent[follower_id] = std::chrono::system_clock::now();
  _lastHighest[follower_id] = highest;
  
  return priv_rpc_ret_t(true, t);
  
}


/// @brief 
bool Agent::activateAgency() {
  if (_config.activeEmpty()) {
    size_t count = 0;
    for (auto const& pair : _config.pool()) {
      _config.activePushBack(pair.first);
      if(++count==size()) {
        break;
      }
    }
    bool persisted = false;
    try {
      persisted = _state.persistActiveAgents(
        _config.activeToBuilder(), _config.poolToBuilder());
    } catch (std::exception const& e) {
      LOG_TOPIC(FATAL, Logger::AGENCY) <<
        "Failed to persist active agency: " << e.what();      
    }
    return persisted;
  }
  return true;
}


/// Load persistent state
bool Agent::load() {
  DatabaseFeature* database =
      ApplicationServer::getFeature<DatabaseFeature>("Database");

  auto vocbase = database->systemDatabase();
  auto queryRegistry = QueryRegistryFeature::QUERY_REGISTRY;

  if (vocbase == nullptr) {
    LOG_TOPIC(FATAL, Logger::AGENCY) << "could not determine _system database";
    FATAL_ERROR_EXIT();
  }

  LOG_TOPIC(DEBUG, Logger::AGENCY) << "Loading persistent state.";
  if (!_state.loadCollections(vocbase, queryRegistry, _config.waitForSync())) {
    LOG_TOPIC(DEBUG, Logger::AGENCY)
        << "Failed to load persistent state on startup.";
  }

  if (size() > 1) {
    _inception->start();
  }

  LOG_TOPIC(DEBUG, Logger::AGENCY) << "Reassembling spearhead and read stores.";
  _spearhead.apply(_state.slices(_lastCommitIndex + 1));

  {
    CONDITION_LOCKER(guard, _appendCV);
    guard.broadcast();
  }

  reportIn(id(), _state.lastLog().index);

  LOG_TOPIC(DEBUG, Logger::AGENCY) << "Starting spearhead worker.";
  _spearhead.start();
  _readDB.start();

  TRI_ASSERT(queryRegistry != nullptr);
  if (size() == 1) {
    activateAgency();
    _constituent.start(vocbase, queryRegistry);
  }

  if (_config.supervision()) {
    LOG_TOPIC(DEBUG, Logger::AGENCY) << "Starting cluster sanity facilities";
    _supervision.start(this);
  }

  return true;
}



/// Challenge my own leadership
bool Agent::challengeLeadership() {

  // Still leading?
  size_t good = 0; 
  for (auto const& i : _lastAcked) {
    std::chrono::duration<double> m =
      std::chrono::system_clock::now() - i.second;
    if(0.9*_config.minPing() > m.count()) {
      ++good;
    }
  }
  return (good < size() / 2); // not counting myself
  
}



/// Write new entries to replicated state and store
write_ret_t Agent::write(query_t const& query) {

  std::vector<bool> applied;
  std::vector<index_t> indices;
  index_t maxind = 0;

  // Only leader else redirect
  if (!_constituent.leading()) {
    return write_ret_t(false, _constituent.leaderID());
  } else {
    if (challengeLeadership()) {
      _constituent.candidate();
      return write_ret_t(false, NO_LEADER);
    }
  }


  // Apply to spearhead and get indices for log entries
  {
    MUTEX_LOCKER(mutexLocker, _ioLock);
    applied = _spearhead.apply(query);
    indices = _state.log(query, applied, term());
  }

  // Maximum log index
  if (!indices.empty()) {
    maxind = *std::max_element(indices.begin(), indices.end());
  }

  // Report that leader has persisted
  reportIn(id(), maxind);
  
  return write_ret_t(true, id(), applied, indices);
  
}


/// Read from store
read_ret_t Agent::read(query_t const& query) {

  MUTEX_LOCKER(mutexLocker, _ioLock);

  // Only leader else redirect
  if (!_constituent.leading()) {
    return read_ret_t(false, _constituent.leaderID());
  } else {
    if (challengeLeadership()) {
      _constituent.candidate();
      return read_ret_t(false, NO_LEADER);
    }
  }

  // Retrieve data from readDB
  auto result = std::make_shared<arangodb::velocypack::Builder>();
  std::vector<bool> success = _readDB.read(query, result);
  
  return read_ret_t(true, _constituent.leaderID(), success, result);

}


/// Send out append entries to followers regularly or on event
void Agent::run() {
  
  CONDITION_LOCKER(guard, _appendCV);

  // Only run in case we are in multi-host mode
  while (!this->isStopping() && size() > 1) {

    if (leading()) {             // Only if leading
      _appendCV.wait(1000);
    } else {
      _appendCV.wait();         // Else wait for our moment in the sun
    }

    // Append entries to followers
    for (auto const& i : _config.active()) {
      if (i != id()) {
        sendAppendEntriesRPC(i);
      }
    }
    
  }

}


/// Orderly shutdown
void Agent::beginShutdown() {
  
  Thread::beginShutdown();

  // Stop supervision
  if (_config.supervision()) {
    _supervision.beginShutdown();
  }

  // Stop constituent and key value stores
  _constituent.beginShutdown();
  _spearhead.beginShutdown();
  _readDB.beginShutdown();
  
  // Wake up all waiting rest handlers
  {
    CONDITION_LOCKER(guardW, _waitForCV);
    guardW.broadcast();
  }
  
  // Wake up run
  {
    CONDITION_LOCKER(guardA, _appendCV);
    guardA.broadcast();
  } 

}


/// Becoming leader
bool Agent::lead() {

  // Key value stores
  rebuildDBs();

  // Wake up run
  {
    CONDITION_LOCKER(guard, _appendCV);
    guard.broadcast();
  }

  for (auto const& i : _config.active()) {
    _lastAcked[i] = std::chrono::system_clock::now();
  }
  
  // Agency configuration
  auto agency = std::make_shared<Builder>();
  agency->openArray();
  agency->openArray();
  agency->openObject();
  agency->add(".agency", VPackValue(VPackValueType::Object));
  agency->add("term", VPackValue(term()));
  agency->add("id", VPackValue(id()));
  agency->add("active", _config.activeToBuilder()->slice());
  agency->add("pool", _config.poolToBuilder()->slice());
  agency->close();  
  agency->close();  
  agency->close();
  agency->close();
  write(agency);

  // Wake up supervision
  _supervision.wakeUp();

  // Notify inactive pool
  notifyInactive();
  
  return true;
  
}


// Notify inactive pool members of configuration change()
void Agent::notifyInactive() const {

  if (_config.poolSize() > _config.size()) {
    
    size_t size = _config.size(),
      counter = 0;
    std::map<std::string,std::string> pool = _config.pool();
    std::string path = "/_api/agency_priv/inform";

    Builder out;
    out.openObject();
    out.add("term", VPackValue(term()));
    out.add("id", VPackValue(id()));
    out.add("active", _config.activeToBuilder()->slice());
    out.add("pool", _config.poolToBuilder()->slice());
    out.close();
    
    for (auto const& p : pool) {
      ++counter;
      if (counter > size) {
        auto headerFields =
          std::make_unique<std::unordered_map<std::string, std::string>>();
        arangodb::ClusterComm::instance()->asyncRequest(
          "1", 1, p.second, arangodb::rest::RequestType::POST,
          path, std::make_shared<std::string>(out.toJson()), headerFields,
          nullptr, 1.0, true);
      }
    }
    
  }
    
}


void Agent::notify(query_t const& message) {

  VPackSlice slice = message->slice();

  if (!slice.isObject()) {
    THROW_ARANGO_EXCEPTION_MESSAGE(
      20011, std::string("Inform message must be an object. Incoming type is ")
      + slice.typeName());
  }

  if (!slice.hasKey("id") || !slice.get("id").isString()) {
    THROW_ARANGO_EXCEPTION_MESSAGE(
      20013, "Inform message must contain string parameter 'id'");
  }
  if (!slice.hasKey("term")) {
    THROW_ARANGO_EXCEPTION_MESSAGE(
      20012, "Inform message must contain uint parameter 'term'");
  }
  _constituent.update(slice.get("id").copyString(), slice.get("term").getUInt());
  
  if (!slice.hasKey("active") || !slice.get("active").isArray()) {
    THROW_ARANGO_EXCEPTION_MESSAGE(
      20014, "Inform message must contain array 'active'");
  }
  if (!slice.hasKey("pool") || !slice.get("pool").isObject()) {
    THROW_ARANGO_EXCEPTION_MESSAGE(
      20015, "Inform message must contain object 'pool'");
  }

  _config.update(message);
  _state.persistActiveAgents(_config.activeToBuilder(), _config.poolToBuilder());

}


// Rebuild key value stores
bool Agent::rebuildDBs() {

  MUTEX_LOCKER(mutexLocker, _ioLock);

  _spearhead.apply(_state.slices(_lastCommitIndex+1));
  _readDB.apply(_state.slices(_lastCommitIndex+1));
  
  return true;
  
}


/// Last commit index
arangodb::consensus::index_t Agent::lastCommitted() const {
  return _lastCommitIndex;
}

/// Last commit index
void Agent::lastCommitted(
  arangodb::consensus::index_t lastCommitIndex) {
  MUTEX_LOCKER(mutexLocker, _ioLock);
  _lastCommitIndex = lastCommitIndex;
}

/// Last log entry
log_t const& Agent::lastLog() const { return _state.lastLog(); }


/// Get spearhead
Store const& Agent::spearhead() const { return _spearhead; }


/// Get readdb
Store const& Agent::readDB() const { return _readDB; }


/// Rebuild from persisted state
Agent& Agent::operator=(VPackSlice const& compaction) {

  // Catch up with compacted state
  MUTEX_LOCKER(mutexLocker, _ioLock);
  _spearhead = compaction.get("readDB");
  _readDB = compaction.get("readDB");

  // Catch up with commit
  try {
    _lastCommitIndex = std::stoul(compaction.get("_key").copyString());
  } catch (std::exception const& e) {
    LOG_TOPIC(ERR, Logger::AGENCY) << e.what() << " " <<__FILE__ << __LINE__;
  }

  // Schedule next compaction
  _nextCompationAfter = _lastCommitIndex + _config.compactionStepSize();

  return *this;
  
}


/// Are we still starting up?
bool Agent::booting() {
  return (!_config.poolComplete());
}


/// We expect an object as follows {id:<id>,endpoint:<endpoint>,pool:{...}}
/// key: uuid value: endpoint
/// Lock configuration and compare
/// Add whatever is missing in our list.
/// Compare whatever is in our list already. (ASSERT identity)
/// If I know more immediately contact peer with my list.
query_t Agent::gossip(query_t const& in, bool isCallback) {

  VPackSlice slice = in->slice();
  if (!slice.isObject()) {
    THROW_ARANGO_EXCEPTION_MESSAGE(
      20001, std::string("Gossip message must be an object. Incoming type is ")
      + slice.typeName());
  }

  if (!slice.hasKey("id") || !slice.get("id").isString()) {
    THROW_ARANGO_EXCEPTION_MESSAGE(
      20002, "Gossip message must contain string parameter 'id'");
  }
  //std::string id = slice.get("id").copyString();
  
  if (!slice.hasKey("endpoint") || !slice.get("endpoint").isString()) {
    THROW_ARANGO_EXCEPTION_MESSAGE(
      20003, "Gossip message must contain string parameter 'endpoint'");
  }
  std::string endpoint = slice.get("endpoint").copyString();
    
  LOG_TOPIC(TRACE, Logger::AGENCY)
    << "Gossip " << ((isCallback) ? "callback" : "call") << " from " << endpoint;
  
  if (!slice.hasKey("pool") || !slice.get("pool").isObject()) {
    THROW_ARANGO_EXCEPTION_MESSAGE(
      20003, "Gossip message must contain object parameter 'pool'");
  }
  VPackSlice pslice = slice.get("pool");

  LOG_TOPIC(TRACE, Logger::AGENCY) <<"Received gossip " << slice.toJson();

  std::map<std::string,std::string> incoming;
  for (auto const& pair : VPackObjectIterator(pslice)) {
    if (!pair.value.isString()) {
      THROW_ARANGO_EXCEPTION_MESSAGE(
        20004, "Gossip message pool must contain string parameters");
    }
    incoming[pair.key.copyString()] = pair.value.copyString();
  }
  
  query_t out = std::make_shared<Builder>();
  out->openObject();
  if (!isCallback) {

    std::vector<std::string> gossipPeers = _config.gossipPeers();
    if (!gossipPeers.empty()) {
      try {
        _config.eraseFromGossipPeers(endpoint);
      } catch (std::exception const& e) {
        LOG_TOPIC(ERR, Logger::AGENCY)
          << __FILE__ << ":" << __LINE__ << " " << e.what();
      }
    }
    
    size_t counter = 0;
    for (auto const& i : incoming) {
      if (++counter > _config.poolSize()) { /// more data than pool size: fatal!
        LOG_TOPIC(FATAL, Logger::AGENCY) <<
          "Too many peers for poolsize: " << counter << ">" << _config.poolSize();
        FATAL_ERROR_EXIT();
      }
      
      if (!_config.addToPool(i)) {
        LOG_TOPIC(FATAL, Logger::AGENCY) << "Discrepancy in agent pool!";
        FATAL_ERROR_EXIT();
      }
    }
    
    
    //bool send = false;
    std::map<std::string,std::string> pool = _config.pool();
    
    out->add("endpoint", VPackValue(_config.endpoint()));
    out->add("id", VPackValue(_config.id()));
    out->add("pool", VPackValue(VPackValueType::Object));
    for (auto const& i : pool) {
      out->add(i.first,VPackValue(i.second));
    }
    out->close();
  }
  out->close();

  LOG_TOPIC(TRACE, Logger::AGENCY)
    << "Answering with gossip " << out->slice().toJson();
  return out;
  
}


}} // namespace
