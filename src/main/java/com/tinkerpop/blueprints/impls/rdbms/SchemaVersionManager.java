// CLASSIFICATION NOTICE: This file is UNCLASSIFIED
package com.tinkerpop.blueprints.impls.rdbms;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SchemaVersionManager {
    public SchemaVersionManager(final DataSource ds) {
        ds_ = ds;
    }
    // =================================
    public void migrate() {
        log.debug("SchemaVersionManager.migrate");

        Flyway flyway = new Flyway();
        flyway.setDataSource(ds_);
        flyway.migrate();
    }
    private DataSource ds_;
}
