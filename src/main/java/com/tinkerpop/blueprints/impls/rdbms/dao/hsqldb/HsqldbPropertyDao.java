package com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb;

import java.util.Set;

import javax.sql.DataSource;

import org.sql2o.Connection;
import org.sql2o.Sql2o;

import com.tinkerpop.blueprints.impls.rdbms.RdbmsEdge;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsGraph;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.PropertyDao;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HsqldbPropertyDao implements PropertyDao {

	HsqldbPropertyDao(DataSource dataSource, RdbmsGraph graph) {
        ds = dataSource;
        this.graph = graph;
        sql2o = new Sql2o(dataSource);
    }
    // =================================
	@Override
	public void set(Object id, String key, Object value) {
		// TODO: this needs to be an upsert
		String sql = "MERGE INTO property AS p " +
		             "USING (VALUES(:id, :key, :value)) AS r(id,key,value) " +
		             "ON p.element_id = r.id AND p.key = r.key " +
		             "WHEN MATCHED THEN " +
		             "      UPDATE SET p.value = r.value " +
		             "WHEN NOT MATCHED THEN " +
		             "    INSERT VALUES r.id, r.key, r.value";
		try (Connection con = sql2o.open()) {
			con.createQuery(sql)
                    .addParameter("id", id)
                    .addParameter("key", key)
                    .addParameter("value", value)
					.executeUpdate();
			log.info("set property {}={} for element id{}", key, value, id);
		}
	}
	// =================================
	@SuppressWarnings("unchecked")
	@Override
	public <T> T get(Object id, String key) {
		log.info("property get {} {}", id, key);
		String sql = "select value from property where element_id = :id and key = :key";
		try (Connection con = sql2o.open()) {
            String val = con.createQuery(sql)
                               .addParameter("id", id)
                               .addParameter("key", key)
                               .executeScalar(String.class);
            log.info("returned prop for {} {}: {}", id, key, val);
            return (T)val;
        }
	}
	// =================================
	@Override
	public <T> T remove(Object id, String key) {
		// TODO Auto-generated method stub
		return null;
	}
	// =================================
	@Override
	public Set<String> keys(Object id) {
		// TODO Auto-generated method stub
		return null;
	}
	// =================================
    private final DataSource ds;
    private final RdbmsGraph graph;
    private final Sql2o sql2o;
}
