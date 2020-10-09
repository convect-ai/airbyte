/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.airbyte.integrations.destination.postgres;

import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.DatabaseHelper;
import io.airbyte.integrations.base.TestDestination;
import java.util.List;
import org.apache.commons.dbcp2.BasicDataSource;
import org.jooq.Record;
import org.testcontainers.containers.PostgreSQLContainer;

public class PostgresIntegrationTest extends TestDestination {

  private PostgreSQLContainer<?> db;

  @Override
  protected String getImageName() {
    return "airbyte/airbyte-postgres-destination:dev";
  }

  @Override
  protected JsonNode getConfig() {
    return Jsons.jsonNode(ImmutableMap.builder()
        .put("host", db.getHost())
        .put("username", db.getUsername())
        .put("password", db.getPassword())
        .put("schema", "public")
        .put("port", db.getFirstMappedPort())
        .put("database", db.getDatabaseName())
        .build());
  }

  @Override
  protected JsonNode getInvalidConfig() {
    return Jsons.jsonNode(ImmutableMap.builder()
        .put("host", db.getHost())
        .put("username", db.getUsername())
        .put("password", "wrong password")
        .put("schema", "public")
        .put("port", db.getFirstMappedPort())
        .put("database", db.getDatabaseName())
        .build());  }

  @Override
  protected List<JsonNode> recordRetriever(TestDestinationEnv testEnv, String streamName) throws Exception {
    BasicDataSource pool =
        DatabaseHelper.getConnectionPool(db.getUsername(), db.getPassword(), db.getJdbcUrl());

    return DatabaseHelper.query(
        pool,
        ctx -> ctx
            .fetch(String.format("SELECT * FROM public.%s ORDER BY inserted_at ASC;", streamName))
            .stream()
            .map(nestedRecords -> ((Record) nestedRecords.get(0))) // todo ?
            .map(Record::intoMap)
            .map(Jsons::jsonNode)
            .collect(toList()));
  }

  @Override
  protected void setup(TestDestinationEnv testEnv) {
    db = new PostgreSQLContainer<>("postgres:13-alpine");
    db.start();
  }

  @Override
  protected void tearDown(TestDestinationEnv testEnv) {
    db.stop();
    db.close();
  }

}
