/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.flume.handlers.socket;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;

/**
 * This handles wired data from udp.
 */
public class UdpSource extends EventSource.Base {
  static final Logger LOG = LoggerFactory.getLogger(UdpSource.class);

  final private static int SYSLOG_UDP_PORT = 514;
  final private static int DEFAULT_BUFFER_SIZE = 10240;

  private int port = SYSLOG_UDP_PORT;
  private int bufferSize = DEFAULT_BUFFER_SIZE;
  private FlumeConfiguration conf;
  private byte[] buffer = null;

  DatagramSocket sock;

  public UdpSource() {
    this(SYSLOG_UDP_PORT);
  }

  public UdpSource(int port) {
    this.port = port;
    conf = FlumeConfiguration.get();
    this.bufferSize = conf.getInt("flume.source.udp.buffer.size",
        DEFAULT_BUFFER_SIZE);
    this.buffer = new byte[bufferSize];
  }

  @Override
  public void open() throws IOException {
    LOG.info("Opening UdpSource on port " + port);
    sock = new DatagramSocket(port);
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing UdpSource on port " + port);
    if (sock != null) {
      sock.close();
    }
  }

  @Override
  public Event next() throws IOException {
    DatagramPacket pkt = new DatagramPacket(buffer, buffer.length);

    sock.receive(pkt);
    byte[] body = new byte[pkt.getLength()];

    System.arraycopy(pkt.getData(), 0, body, 0, pkt.getLength());
    Event e = new EventImpl(body);
    updateEventProcessingStats(e);
    return e;
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {

      @Override
      public EventSource build(Context ctx, String... argv) {
        int port = SYSLOG_UDP_PORT;
        if (argv.length != 1 ) {
          throw new IllegalArgumentException("usage: udp(port)");
        }
        port = Integer.parseInt(argv[0]);
        return new UdpSource(port);
      }
    };
  }
}
