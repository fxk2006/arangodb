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

#include "SocketTask2.h"

#include "Basics/StringBuffer.h"
#include "Basics/socket-utils.h"
#include "Logger/Logger.h"
#include "Scheduler/Scheduler.h"

#include <errno.h>

using namespace arangodb::basics;
using namespace arangodb::rest;

void disableNagle(int sock) {
  int x = 1;

  #ifdef SOL_TCP
  int level = SOL_TCP;
  #else
  int level = SOL_SOCKET;
  #endif

  if (setsockopt(sock, level, TCP_NODELAY, (char*)&x, sizeof(x)))
    LOG(ERR) << "disableNagle failed: ";

  #ifdef SO_KEEPALIVE
  if (setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, (char*)&x, sizeof(x)))
    LOG(ERR) << "SO_KEEPALIVE failed: ";

  #ifdef __linux__
  socklen_t len = sizeof(x);
  if (getsockopt(sock, level, TCP_KEEPIDLE, (char*)&x, &len))
    LOG(ERR) << "can't get TCP_KEEPIDLE: ";

  if (x > 300) {
    x = 300;
    if (setsockopt(sock, level, TCP_KEEPIDLE, (char*)&x, sizeof(x))) {
      LOG(ERR) << "can't set TCP_KEEPIDLE: ";
    }
  }

  len = sizeof(x);  // just in case it changed
  if (getsockopt(sock, level, TCP_KEEPINTVL, (char*)&x, &len))
    LOG(ERR) << "can't get TCP_KEEPINTVL: ";

  if (x > 300) {
    x = 300;
    if (setsockopt(sock, level, TCP_KEEPINTVL, (char*)&x, sizeof(x))) {
      LOG(ERR) << "can't set TCP_KEEPINTVL: ";
    }
  }
  #endif
  #endif
}

SocketTask2::SocketTask2(EventLoop2 loop, TRI_socket_t socket,
                         double keepAliveTimeout)
    : Task2(loop, "SocketTask2"), _stream(loop._ioService) {
  _readBuffer = new StringBuffer(TRI_UNKNOWN_MEM_ZONE, false);

  ConnectionStatisticsAgent::acquire();
  connectionStatisticsAgentSetStart();

  disableNagle(socket.fileDescriptor);

  boost::system::error_code ec;
  _stream.assign(boost::asio::ip::tcp::v4(), socket.fileDescriptor, ec);

  _stream.non_blocking(false);
  
  if (ec) {
    LOG_TOPIC(ERR, Logger::COMMUNICATION) << "cannot create stream from socket"
                                          << ec;
    _closedSend = true;
    _closedReceive = true;
  } else {
    LOG_TOPIC(ERR, Logger::COMMUNICATION) << "FIXME ASSIGNED "
                                          << socket.fileDescriptor;
  }
}

void SocketTask2::start() {
  if (_closedSend || _closedReceive) {
    LOG_TOPIC(DEBUG, Logger::COMMUNICATION) << "cannot start, channel closed";
    return;
  }

  if (_closeRequested) {
    LOG_TOPIC(DEBUG, Logger::COMMUNICATION)
        << "cannot start, close alread in progress";
    return;
  }

  LOG(ERR) << "### startRead";
  _loop._ioService.post([this]() { asyncReadSome(); });
}

void SocketTask2::asyncReadSome() {
  while (true) {
    // reserve some memory for reading
    if (_readBuffer->reserve(READ_BLOCK_SIZE + 1) == TRI_ERROR_OUT_OF_MEMORY) {
      LOG(WARN) << "out of memory while reading from client";
      closeStream();
      return;
    }

    try {
      LOG_TOPIC(INFO, Logger::REQUESTS)
	<< "vor_read_some;"
	<< uint64_t(TRI_microtime() * 1000000) << ";"
	<< Thread::currentThreadId() << ";";
      auto xxx = boost::asio::buffer(_readBuffer->end(), READ_BLOCK_SIZE);
      LOG_TOPIC(INFO, Logger::REQUESTS)
	<< "direkt_vor_read_some;"
	<< uint64_t(TRI_microtime() * 1000000) << ";"
	<< Thread::currentThreadId() << ";";
      size_t bytesRead = _stream.read_some(xxx);
      LOG_TOPIC(INFO, Logger::REQUESTS)
	<< "nach_read_some;"
	<< uint64_t(TRI_microtime() * 1000000) << ";"
	<< Thread::currentThreadId() << ";"
        << bytesRead << ";";

      //LOG(ERR) << "INPUT: " << StringUtils::escapeC(std::string(_readBuffer->end(), bytesRead))
      // << " " << bytesRead << " " << _readBuffer->end();

      if (0 < bytesRead) {
	_readBuffer->increaseLength(bytesRead);

	while (processRead()) {
	  if (_closeRequested) {
	    break;
	  }
	}
  
	if (_closeRequested) {
	  LOG_TOPIC(DEBUG, Logger::COMMUNICATION)
	    << "close requested, closing receive stream";

	  closeReceiveStream();
	  return;
	} else {
	  continue;
	}
      }
    } catch (boost::system::system_error err) {
      if (err.code() != boost::asio::error::would_block) {
	LOG(ERR) << "CLOSED NO WOULD BLOCK " << err.what();
	closeStream();
	return;
      }
    }

    break;
  }

  // try to read more bytes
  _stream.async_read_some(
      boost::asio::buffer(_readBuffer->end(), READ_BLOCK_SIZE),
      [this](const boost::system::error_code& ec, std::size_t transferred) {
        if (ec) {
          LOG_TOPIC(DEBUG, Logger::COMMUNICATION)
              << "read on stream " << _stream.native_handle() << " failed with "
              << ec;
          closeStream();
        } else {
          _readBuffer->increaseLength(transferred);

          while (processRead()) {
            if (_closeRequested) {
              break;
            }
          }

          if (_closeRequested) {
            LOG_TOPIC(DEBUG, Logger::COMMUNICATION)
                << "close requested, closing receive stream";

            closeReceiveStream();
          } else {
            asyncReadSome();
          }
        }
      });
}

void SocketTask2::closeStream() {
  if (!_closedSend) {
    try {
      _stream.shutdown(boost::asio::ip::tcp::socket::shutdown_send);
    } catch (boost::system::system_error err) {
      LOG(WARN) << "shutdown send stream " << _stream.native_handle()
                << " failed with " << err.what();
    }

    _closedSend = true;
  }

  if (!_closedReceive) {
    try {
      _stream.shutdown(boost::asio::ip::tcp::socket::shutdown_receive);
    } catch (boost::system::system_error err) {
      LOG(WARN) << "shutdown send stream " << _stream.native_handle()
                << " failed with " << err.what();
    }

    _closedReceive = true;
  }

  try {
    _stream.close();
  } catch (boost::system::system_error err) {
    LOG(WARN) << "close stream " << _stream.native_handle() << " failed with "
              << err.what();
  }

  _closeRequested = false;
}

void SocketTask2::closeReceiveStream() {
  if (!_closedReceive) {
    try {
      _stream.shutdown(boost::asio::ip::tcp::socket::shutdown_receive);
    } catch (boost::system::system_error err) {
      LOG(WARN) << "shutdown receive stream " << _stream.native_handle()
                << " failed with " << err.what();
    }

    _closedReceive = true;
  }
}

void SocketTask2::addWriteBuffer(std::unique_ptr<basics::StringBuffer> buffer,
                                 RequestStatisticsAgent* statistics) {
  TRI_request_statistics_t* stat =
      statistics == nullptr ? nullptr : statistics->steal();

  addWriteBuffer(buffer.release(), stat);
}

#include <iostream>

void SocketTask2::addWriteBuffer(basics::StringBuffer* buffer,
                                 TRI_request_statistics_t* stat) {
  LOG_TOPIC(INFO, Logger::REQUESTS)
    << "stop;"
    << uint64_t(TRI_microtime() * 1000000) << ";"
    << (void*) this << ";"
    << Thread::currentThreadId() << ";";

  if (_closedSend) {
    LOG_TOPIC(DEBUG, Logger::COMMUNICATION)
        << "dropping output, send stream already closed";

    delete buffer;

    if (stat) {
      TRI_ReleaseRequestStatistics(stat);
    }

    return;
  }

  if (_writeBuffer != nullptr) {
    _writeBuffers.push_back(buffer);
    _writeBuffersStats.push_back(stat);
    return;
  }

  _writeBuffer = buffer;
  _writeBufferStatistics = stat;

  if (_writeBuffer != nullptr) {
    boost::system::error_code ec;
    size_t total = _writeBuffer->length();
    size_t written = 0;

    // LOG(ERR) << "OUTPUT: " << StringUtils::escapeC(std::string(_writeBuffer->begin(), _writeBuffer->length())) << " " << _writeBuffer->length();

    try {
      written = _stream.write_some(boost::asio::buffer(_writeBuffer->begin(), _writeBuffer->length()));

      if (written == total) {
	LOG_TOPIC(INFO, Logger::REQUESTS)
	  << "stop2;"
	  << uint64_t(TRI_microtime() * 1000000) << ";"
	  << (void*) this << ";"
	  << Thread::currentThreadId() << ";";

	completedWriteBuffer();
	return;
      }
    } catch (boost::system::system_error err) {
      if (err.code() != boost::asio::error::would_block) {
	LOG_TOPIC(DEBUG, Logger::COMMUNICATION) << "write on stream "
						<< _stream.native_handle()
						<< " failed with " << err.what();
	closeStream();
	return;
      }
    }

    LOG(ERR) << "NO DIRECT";

    boost::asio::async_write(
        _stream,
        boost::asio::buffer(_writeBuffer->begin() + written, total - written),
        [this](const boost::system::error_code& ec, std::size_t transferred) {
          if (ec) {
            LOG_TOPIC(DEBUG, Logger::COMMUNICATION) << "write on stream "
                                                    << _stream.native_handle()
                                                    << " failed with " << ec;
            closeStream();
          } else {
            completedWriteBuffer();
          }
        });
  }
}

void SocketTask2::completedWriteBuffer() {
  delete _writeBuffer;
  _writeBuffer = nullptr;

  if (_writeBufferStatistics != nullptr) {
    _writeBufferStatistics->_writeEnd = TRI_StatisticsTime();
    TRI_ReleaseRequestStatistics(_writeBufferStatistics);
    _writeBufferStatistics = nullptr;
  }

  if (_writeBuffers.empty()) {
    if (_closeRequested) {
      LOG_TOPIC(DEBUG, Logger::COMMUNICATION)
          << "close requested, closing stream";

      closeStream();
    }

    return;
  }

  StringBuffer* buffer = _writeBuffers.front();
  _writeBuffers.pop_front();

  TRI_request_statistics_t* statistics = _writeBuffersStats.front();
  _writeBuffersStats.pop_front();

  addWriteBuffer(buffer, statistics);
}
