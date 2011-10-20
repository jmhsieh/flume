/*
 * Copyright 2011 The Apache Software Foundation
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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.CompositeSink;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.util.Clock;

/**
 * Test UDP source.
 */
public class TestUdpSource {
  final public static Logger LOG = Logger.getLogger(TestUdpSource.class);

  final private int port = 12345;

  void runDriver(final EventSource src, final EventSink snk,
      final CountDownLatch done, final int count) {
    final Thread workerThread = new Thread() {

      @Override
      public void run() {
        try {
          LOG.info("opening src");
          src.open();
          LOG.info("opening snk");
          snk.open();

          EventUtil.dumpN(count, src, snk);
          Clock.sleep(500);
          LOG.info("closing src");
          src.close();
          LOG.info("closing snk");
          snk.close();
        } catch (Exception e) {
          LOG.error("Unexpected exception", e);
        } finally {
          LOG.info("triggering latch");
          done.countDown();
        }
      }
    };
    workerThread.start();
  }

  /**
   * Send some packets thru udp.
   */
  @Test
  public void testTailSource() throws IOException, FlumeSpecException,
      InterruptedException {
    final CompositeSink snk = new CompositeSink(new ReportTestingContext(),
        "{ delay(50) => counter(\"count\") }");
    final EventSource src = UdpSource.builder().create(
        LogicalNodeContext.testingContext(), port);
    final CountDownLatch done = new CountDownLatch(1);
    final int count = 100;
    runDriver(src, snk, done, count);

    // send udp packets
    final DatagramSocket socket = new DatagramSocket();
    final byte[] b = new byte[256];

    for (int i = 0; i < count; ++i) {
      Arrays.fill(b, (byte)'a');
      DatagramPacket dp = new DatagramPacket(b, b.length,
          InetAddress.getByName("localhost"), port);
      socket.send(dp);
    }
    socket.close();

    assertTrue(done.await(30, TimeUnit.SECONDS));

    CounterSink ctr = (CounterSink) ReportManager.get().getReportable("count");

    // Just assert received # of event, since UDP may loss data.
    assertTrue("expected to receive " + count +
        " events, but actually received " +
        ctr.getCount(), count == ctr.getCount());
  }
}
