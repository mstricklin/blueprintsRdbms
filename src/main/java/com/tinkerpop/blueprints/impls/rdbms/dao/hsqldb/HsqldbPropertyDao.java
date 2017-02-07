package com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb;

import static com.google.common.collect.Maps.newHashMap;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.Map.Entry;

import javax.sql.DataSource;

import org.sql2o.Connection;
import org.sql2o.Sql2o;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.impls.rdbms.RdbmsGraph;
import com.tinkerpop.blueprints.impls.rdbms.dao.DaoFactory.PropertyDao;
import com.tinkerpop.blueprints.impls.rdbms.dao.hsqldb.HsqldbKryoDao.ClassRegistration;
import com.tinkerpop.blueprints.impls.rdbms.dao.Serializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HsqldbPropertyDao implements PropertyDao {

	HsqldbPropertyDao(DataSource dataSource, RdbmsGraph graph, Serializer serializer) {
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
			log.info("set property {}={} for element id {}", key, value, id);
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
	public void remove(Object id, String key) {
		String sql = "delete from property " +
		             "where element_id = :id and key = :key";
		try (Connection con = sql2o.open()) {
            con.createQuery(sql).addParameter("id", id)
                                .addParameter("key", key)
                                .executeUpdate();
            log.info("deleted prop for {} {}", id, key);
        }
	}
	// =================================
	@Override
	public Set<String> keys(Object id) {
		// TODO Auto-generated method stub
		return null;
	}
	// =================================
    static final class Property  {
		String key;
        String value;
        static Function<Property, Map.Entry<String, String>> makeEntry
                 = new Function<Property, Map.Entry<String, String>>() {
			@Override
			public Entry<String, String> apply(Property p) {
				return new SimpleImmutableEntry<String, String>(p.key, p.value);
			}
        };
    }
	// =================================
	@Override
	public Map<String, Object> values(Object id) {
        String sql = "select key, value from property " +
	                 "where element_id = :id";

        try (Connection con = sql2o.open()) {
        	log.info("get all properties for {}", id);
            List<Property> l = con.createQuery(sql)
            		              .addParameter("id", id)
                                  .executeAndFetch(Property.class);
            ImmutableMap<String, String> m = ImmutableMap.copyOf( Iterables.transform(l, Property.makeEntry) );
            log.info("props: {}", m);
//            return Property.makeMap(l);
            return Collections.emptyMap();
        }
	}
	// =================================
    private final DataSource ds;
    private final RdbmsGraph graph;
    private final Sql2o sql2o;
}
