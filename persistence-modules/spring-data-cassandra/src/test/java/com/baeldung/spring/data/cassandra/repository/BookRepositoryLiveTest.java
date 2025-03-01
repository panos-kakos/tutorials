package com.baeldung.spring.data.cassandra.repository;

import com.baeldung.spring.data.cassandra.config.CassandraConfig;
import com.baeldung.spring.data.cassandra.model.Book;
import com.baeldung.spring.data.cassandra.repository.BookRepository;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Live test for Cassandra testing.
 *
 * This can be converted to IntegrationTest once cassandra-unit tests can be executed in parallel and
 * multiple test servers started as part of test suite.
 *
 * Open cassandra-unit issue for parallel execution: https://github.com/jsevellec/cassandra-unit/issues/155
 */
@SpringBootTest(classes = CassandraConfig.class)
public class BookRepositoryLiveTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(BookRepositoryLiveTest.class);

    public static final String KEYSPACE_CREATION_QUERY = "CREATE KEYSPACE IF NOT EXISTS testKeySpace " +
        "WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' };";

    public static final String KEYSPACE_ACTIVATE_QUERY = "USE testKeySpace;";

    public static final String DATA_TABLE_NAME = "book";

    @Autowired
    private BookRepository bookRepository;

    @Autowired
    private CassandraAdminTemplate adminTemplate;

    @BeforeAll
    public static void startCassandraEmbedded() throws InterruptedException, TTransportException, ConfigurationException, IOException {
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

    @BeforeEach
    public void createTable() {
        adminTemplate.createTable(true, Book.class);
    }

    @Test
    public void whenSavingBook_thenAvailableOnRetrieval() {
        final Book javaBook = new Book(generateTimeBasedUUID(), "Head First Java",
            "O'Reilly Media", ImmutableSet.of("Computer", "Software"));
        bookRepository.save(javaBook);

        final List<Book> books = bookRepository.findByTitleAndPublisher("Head First Java", "O'Reilly Media");
        assertEquals(javaBook.getId(), books.get(0).getId());
    }

    @Test
    public void whenUpdatingBooks_thenAvailableOnRetrieval() {
        final Book javaBook = new Book(generateTimeBasedUUID(), "Head First Java",
            "O'Reilly Media", ImmutableSet.of("Computer", "Software"));
        bookRepository.save(javaBook);

        javaBook.setTitle("Head First Java Second Edition");
        bookRepository.save(javaBook);

        final List<Book> updatedBooks = bookRepository.findByTitleAndPublisher(
            "Head First Java Second Edition", "O'Reilly Media");
        assertEquals(javaBook.getTitle(), updatedBooks.get(0).getTitle());
    }

    @Test
    public void whenDeletingExistingBooks_thenNotAvailableOnRetrieval() {
        final Book javaBook = new Book(generateTimeBasedUUID(), "Head First Java",
            "O'Reilly Media", ImmutableSet.of("Computer", "Software"));
        bookRepository.save(javaBook);
        bookRepository.delete(javaBook);

        final List<Book> books = bookRepository.findByTitleAndPublisher("Head First Java", "O'Reilly Media");
        assertThrows(IndexOutOfBoundsException.class, () -> books.get(0));
    }

    @Test
    public void whenSavingBooks_thenAllShouldAvailableOnRetrieval() {
        final Book javaBook = new Book(generateTimeBasedUUID(), "Head First Java",
            "O'Reilly Media", ImmutableSet.of("Computer", "Software"));
        final Book dPatternBook = new Book(generateTimeBasedUUID(), "Head Design Patterns",
            "O'Reilly Media", ImmutableSet.of("Computer", "Software"));

        bookRepository.save(javaBook);
        bookRepository.save(dPatternBook);

        final Iterable<Book> books = bookRepository.findAll();
        int bookCount = 0;
        for (final Book book : books) {
            bookCount++;
        }
        assertEquals(2, bookCount);
    }

    @AfterEach
    public void dropTable() {
        adminTemplate.dropTable(CqlIdentifier.fromCql(DATA_TABLE_NAME));
    }

    @AfterAll
    public static void stopCassandraEmbedded() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    private UUID generateTimeBasedUUID() {
        return com.datastax.oss.driver.api.core.uuid.Uuids.timeBased();
    }
}