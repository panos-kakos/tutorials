package com.baeldung.spring.data.cassandra.repository;

import com.baeldung.spring.data.cassandra.config.CassandraConfig;
import com.baeldung.spring.data.cassandra.model.Book;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.core.CassandraOperations;


import java.io.IOException;
import java.util.ArrayList;

import java.util.List;

import static junit.framework.TestCase.assertNull;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Live test for Cassandra testing.
 *
 * This can be converted to IntegrationTest once cassandra-unit tests can be executed in parallel and
 * multiple test servers started as part of test suite.
 *
 * Open cassandra-unit issue for parallel execution: https://github.com/jsevellec/cassandra-unit/issues/155
 */
@SpringBootTest(classes = CassandraConfig.class)
public class CassandraTemplateLiveTest {
    private static final Log LOGGER = LogFactory.getLog(CassandraTemplateLiveTest.class);

    public static final String KEYSPACE_CREATION_QUERY = "CREATE KEYSPACE IF NOT EXISTS testKeySpace " + "WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' };";

    public static final String KEYSPACE_ACTIVATE_QUERY = "USE testKeySpace;";

    public static final String DATA_TABLE_NAME = "book";

    @Autowired
    private CassandraAdminTemplate adminTemplate;

    @Autowired
    private CassandraOperations cassandraTemplate;

    @BeforeAll
    public static void startCassandraEmbedded() throws InterruptedException, TTransportException, ConfigurationException, IOException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        final Cluster cluster = Cluster.builder().addContactPoints("127.0.0.1").withPort(9142).build();
        LOGGER.info("Server Started at 127.0.0.1:9142... ");
        final Session session = cluster.connect();
        session.execute(KEYSPACE_CREATION_QUERY);
        session.execute(KEYSPACE_ACTIVATE_QUERY);
        LOGGER.info("KeySpace created and activated.");
        Thread.sleep(5000);
    }

    @BeforeEach
    public void createTable() {
        adminTemplate.createTable(true, Book.class);
    }

    @Test
    public void whenSavingBook_thenAvailableOnRetrieval() {
        final Book javaBook = new Book(UUIDs.timeBased(), "Head First Java", "O'Reilly Media", ImmutableSet.of("Computer", "Software"));
        cassandraTemplate.insert(javaBook);

        Select select = QueryBuilder.selectFrom(DATA_TABLE_NAME)
                .all()
                .whereColumn("title").isEqualTo(QueryBuilder.literal("Head First Java"))
                .whereColumn("publisher").isEqualTo(QueryBuilder.literal("O'Reilly Media"))
                .limit(10);

        final Book retrievedBook = cassandraTemplate.selectOne(
                SimpleStatement.newInstance(select.build().getQuery()), Book.class);
        assertEquals(javaBook.getId(), retrievedBook.getId());
    }

    @Test
    public void whenSavingBooks_thenAllAvailableOnRetrieval() {
        final Book javaBook = new Book(UUIDs.timeBased(), "Head First Java", "O'Reilly Media", ImmutableSet.of("Computer", "Software"));
        final Book dPatternBook = new Book(UUIDs.timeBased(), "Head Design Patterns", "O'Reilly Media", ImmutableSet.of("Computer", "Software"));
        final List<Book> bookList = new ArrayList<>();
        bookList.add(javaBook);
        bookList.add(dPatternBook);
        cassandraTemplate.insert(bookList);

        Select select = QueryBuilder.selectFrom(DATA_TABLE_NAME).all().limit(10);
        final List<Book> retrievedBooks = cassandraTemplate.select(
                SimpleStatement.newInstance(select.build().getQuery()), Book.class);
        assertThat(retrievedBooks.size(), is(2));
        assertEquals(javaBook.getId(), retrievedBooks.get(0).getId());
        assertEquals(dPatternBook.getId(), retrievedBooks.get(1).getId());
    }

    @Test
    public void whenUpdatingBook_thenShouldUpdatedOnRetrieval() {
        final Book javaBook = new Book(UUIDs.timeBased(), "Head First Java", "O'Reilly Media", ImmutableSet.of("Computer", "Software"));
        cassandraTemplate.insert(javaBook);
        Select select = QueryBuilder.selectFrom(DATA_TABLE_NAME).all().limit(10);
        final Book retrievedBook = cassandraTemplate.selectOne(
                SimpleStatement.newInstance(select.build().getQuery()), Book.class);
        retrievedBook.setTags(ImmutableSet.of("Java", "Programming"));
        cassandraTemplate.update(retrievedBook);
        final Book retrievedUpdatedBook = cassandraTemplate.selectOne(
                SimpleStatement.newInstance(select.build().getQuery()), Book.class);
        assertEquals(retrievedBook.getTags(), retrievedUpdatedBook.getTags());
    }

    @Test
    public void whenDeletingASelectedBook_thenNotAvailableOnRetrieval() {
        final Book javaBook = new Book(UUIDs.timeBased(), "Head First Java", "OReilly Media", ImmutableSet.of("Computer", "Software"));
        cassandraTemplate.insert(javaBook);
        cassandraTemplate.delete(javaBook);
        Select select = QueryBuilder.selectFrom(DATA_TABLE_NAME).all().limit(10);
        final Book retrievedUpdatedBook = cassandraTemplate.selectOne(
                SimpleStatement.newInstance(select.build().getQuery()), Book.class);
        assertNull(retrievedUpdatedBook);
    }

    @Test
    public void whenDeletingAllBooks_thenNotAvailableOnRetrieval() {
        final Book javaBook = new Book(UUIDs.timeBased(), "Head First Java", "O'Reilly Media", ImmutableSet.of("Computer", "Software"));
        final Book dPatternBook = new Book(UUIDs.timeBased(), "Head Design Patterns", "O'Reilly Media", ImmutableSet.of("Computer", "Software"));
        cassandraTemplate.insert(javaBook);
        cassandraTemplate.insert(dPatternBook);
        cassandraTemplate.delete(Book.class);
        Select select = QueryBuilder.selectFrom(DATA_TABLE_NAME).all().limit(10);
        final Book retrievedUpdatedBook = cassandraTemplate.selectOne(
                SimpleStatement.newInstance(select.build().getQuery()), Book.class);
        assertNull(retrievedUpdatedBook);
    }

    @Test
    public void whenAddingBooks_thenCountShouldBeCorrectOnRetrieval() {
        final Book javaBook = new Book(UUIDs.timeBased(), "Head First Java", "O'Reilly Media", ImmutableSet.of("Computer", "Software"));
        final Book dPatternBook = new Book(UUIDs.timeBased(), "Head Design Patterns", "O'Reilly Media", ImmutableSet.of("Computer", "Software"));
        cassandraTemplate.insert(javaBook);
        cassandraTemplate.insert(dPatternBook);
        final long bookCount = cassandraTemplate.count(Book.class);
        assertEquals(2, bookCount);
    }

    @AfterEach
    public void dropTable() {
        adminTemplate.dropTable(Book.class);
    }

    @AfterAll
    public static void stopCassandraEmbedded() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }
}
