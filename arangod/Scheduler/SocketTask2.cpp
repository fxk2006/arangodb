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

SocketTask2::SocketTask2(EventLoop2 loop, TRI_socket_t socket,
                         double keepAliveTimeout)
    : Task2(loop, "SocketTask2"), _stream(loop._ioService) {
  _readBuffer = new StringBuffer(TRI_UNKNOWN_MEM_ZONE, false);

  ConnectionStatisticsAgent::acquire();
  connectionStatisticsAgentSetStart();

  boost::system::error_code ec;
  _stream.assign(boost::asio::ip::tcp::v4(), socket.fileDescriptor, ec);

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
  // reserve some memory for reading
  if (_readBuffer->reserve(READ_BLOCK_SIZE + 1) == TRI_ERROR_OUT_OF_MEMORY) {
    LOG(WARN) << "out of memory while reading from client";
    closeStream();
    return;
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
    } catch (boost::system::system_error ec) {
      LOG(WARN) << "shutdown send stream " << _stream.native_handle()
                << " failed with " << ec.what();
    }

    _closedSend = true;
  }

  if (!_closedReceive) {
    try {
      _stream.shutdown(boost::asio::ip::tcp::socket::shutdown_receive);
    } catch (boost::system::system_error ec) {
      LOG(WARN) << "shutdown send stream " << _stream.native_handle()
                << " failed with " << ec.what();
    }

    _closedReceive = true;
  }

  try {
    _stream.close();
  } catch (boost::system::system_error ec) {
    LOG(WARN) << "close stream " << _stream.native_handle() << " failed with "
              << ec.what();
  }

  _closeRequested = false;
}

void SocketTask2::closeReceiveStream() {
  if (!_closedReceive) {
    try {
      _stream.shutdown(boost::asio::ip::tcp::socket::shutdown_receive);
    } catch (boost::system::system_error ec) {
      LOG(WARN) << "shutdown receive stream " << _stream.native_handle()
                << " failed with " << ec.what();
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

void SocketTask2::addWriteBuffer(basics::StringBuffer* buffer,
                                 TRI_request_statistics_t* stat) {
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
    boost::asio::async_write(
        _stream,
        boost::asio::buffer(_writeBuffer->begin(), _writeBuffer->length()),
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
