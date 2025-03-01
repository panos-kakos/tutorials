package com.baeldung.spring.data.cassandra.repository;

import com.baeldung.spring.data.cassandra.config.CassandraConfig;
import com.baeldung.spring.data.cassandra.model.Book;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static junit.framework.TestCase.assertEquals;

/**
 * Live test for Cassandra testing.
 *
 * This can be converted to IntegrationTest once cassandra-unit tests can be executed in parallel and
 * multiple test servers started as part of test suite.
 *
 * Open cassandra-unit issue for parallel execution: https://github.com/jsevellec/cassandra-unit/issues/155
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = CassandraConfig.class)
public class CqlQueriesLiveTest {
    private static final Log LOGGER = LogFactory.getLog(CqlQueriesLiveTest.class);

    public static final String KEYSPACE_CREATION_QUERY = "CREATE KEYSPACE IF NOT EXISTS testKeySpace " + "WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' };";

    public static final String KEYSPACE_ACTIVATE_QUERY = "USE testKeySpace;";

    public static final String DATA_TABLE_NAME = "book";

    @Autowired
    private CassandraAdminOperations adminTemplate;

    @Autowired
    private CassandraOperations cassandraTemplate;

    @BeforeClass
    public static void startCassandraEmbedded() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        try (CqlSession session = CqlSession.builder()
            .addContactPoint(new java.net.InetSocketAddress("127.0.0.1", 9142))
            .withLocalDatacenter("datacenter1")
            .build()) {
            LOGGER.info("Server Started at 127.0.0.1:9142... ");
            session.execute(KEYSPACE_CREATION_QUERY);
            session.execute(KEYSPACE_ACTIVATE_QUERY);
            LOGGER.info("KeySpace created and activated.");
        }
        Thread.sleep(5000);
    }

    @Before
    public void createTable() {
        adminTemplate.createTable(true, Book.class);
    }

    @Test
    public void whenSavingBook_thenAvailableOnRetrieval_usingQueryBuilder() {
        final UUID uuid = generateTimeBasedUUID();
        final Insert insert = QueryBuilder.insertInto(DATA_TABLE_NAME)
            .value("id", uuid)
            .value("title", "Head First Java")
            .value("publisher", "OReilly Media")
            .value("tags", ImmutableSet.of("Software"));

        cassandraTemplate.execute(insert.);
        final Select select = QueryBuilder.select().from("book").limit(10);
        final Book retrievedBook = cassandraTemplate.selectOne(select, Book.class);
        assertEquals(uuid, retrievedBook.getId());
    }

    @Test
    public void whenSavingBook_thenAvailableOnRetrieval_usingCQLStatements() {
        final UUID uuid = UUIDs.timeBased();
        final String insertCql = "insert into book (id, title, publisher, tags) values " + "(" + uuid + ", 'Head First Java', 'OReilly Media', {'Software'})";
        cassandraTemplate.execute(insertCql);
        final Select select = QueryBuilder.select().from("book").limit(10);
        final Book retrievedBook = cassandraTemplate.selectOne(select, Book.class);
        assertEquals(uuid, retrievedBook.getId());
    }

    @Test
    public void whenSavingBook_thenAvailableOnRetrieval_usingPreparedStatements() throws InterruptedException {
        final UUID uuid = UUIDs.timeBased();
        final String insertPreparedCql = "insert into book (id, title, publisher, tags) values (?, ?, ?, ?)";
        final List<Object> singleBookArgsList = new ArrayList<>();
        final List<List<?>> bookList = new ArrayList<>();
        singleBookArgsList.add(uuid);
        singleBookArgsList.add("Head First Java");
        singleBookArgsList.add("OReilly Media");
        singleBookArgsList.add(ImmutableSet.of("Software"));
        bookList.add(singleBookArgsList);
        cassandraTemplate.insert(insertPreparedCql, bookList);
        // This may not be required, just added to avoid any transient issues
        Thread.sleep(5000);
        final Select select = QueryBuilder.select().from("book");
        final Book retrievedBook = cassandraTemplate.selectOne(select, Book.class);
        assertEquals(uuid, retrievedBook.getId());
    }

    @After
    public void dropTable() {
        adminTemplate.dropTable(Book.class);
    }

    @AfterClass
    public static void stopCassandraEmbedded() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    private UUID generateTimeBasedUUID() {
        return  Uuids.timeBased();
    }
}
