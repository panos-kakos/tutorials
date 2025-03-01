package com.baeldung.spring.data.cassandra.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.data.cassandra.config.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

import java.util.Arrays;
import java.util.List;

@Configuration
@PropertySource(value = { "classpath:cassandra.properties" })
@EnableCassandraRepositories(basePackages = "com.baeldung.spring.data.cassandra.repository")
public class CassandraConfig extends AbstractCassandraConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraConfig.class);

    @Autowired
    private Environment environment;

    @Override
    protected String getKeyspaceName() {
        return environment.getProperty("cassandra.keyspace");
    }

    @Override
    protected String getContactPoints() {
        return environment.getProperty("cassandra.contactpoints");
    }

    @Override
    protected int getPort() {
        return Integer.parseInt(environment.getProperty("cassandra.port"));
    }

    @Override
    public SchemaAction getSchemaAction() {
        return SchemaAction.CREATE_IF_NOT_EXISTS;
    }

    @Override
    protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
        CreateKeyspaceSpecification specification = CreateKeyspaceSpecification
            .createKeyspace(getKeyspaceName())
            .ifNotExists()
            .withSimpleReplication();

        LOGGER.info("Cluster created with contact points [{}] & port [{}].",
            environment.getProperty("cassandra.contactpoints"),
            Integer.parseInt(environment.getProperty("cassandra.port")));

        return Arrays.asList(specification);
    }
}