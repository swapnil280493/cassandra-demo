package com.example.cassandraDemo.dao;

import com.datastax.driver.core.*;
import com.example.cassandraDemo.model.Record;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.*;

@Repository
public class RecordDao {
    @Autowired
    CassandraConnectionUtil cassandraConnectionUtil;


    //extends CassandraRepository<SearchDetails, String> {
    Session session = null;
    Map<String, Session> sessionMap = new HashMap<>();
    Map<String, PreparedStatement> Statement = new HashMap<>();

    @Autowired
    public RecordDao(CassandraConnectionUtil cassandraConnectionUtil) {
        this.cassandraConnectionUtil = cassandraConnectionUtil;
        this.sessionMap = getSessionHandle();
    }

    private Map<String, Session> getSessionHandle() {
        Map<String, Session> sessionMap1 = new HashMap<>();
        sessionMap1.put("versionindexer", cassandraConnectionUtil.getSession());
        // sessionMap1.put("tutorialspoint",cassandraConnectionUtil.getSession("tutorialspoint"));
        return sessionMap1;
    }

    public void closeSession() {
        if (session != null)
            session.close();
        session = null;
    }

    public List<Record> findAll(String keyspace) {
        session = sessionMap.get(keyspace);
        List<Record> cursorDetailsList = new ArrayList<>();
        PreparedStatement preparedStatement = session.prepare(String.format("SELECT * FROM %s.VARIATIONS ALLOW FILTERING", keyspace));
        // Bind the parameters and execute the statement
        ResultSet resultSet = session.execute(preparedStatement.bind());
        for (com.datastax.driver.core.Row row : resultSet) {
            cursorDetailsList.add(new Record(row.getString("RECORD_VERSION"), row.getLong("RECORD_VERSION"), row.getString("TECH_ASSURANCE")));
        }
        return cursorDetailsList;
    }

    public String delete(String keyspace, String recordid) {
        session = sessionMap.get(keyspace);
        PreparedStatement preparedStatement = session.prepare(String.format("DELETE FROM %s.records where recordid=?", keyspace));
        BoundStatement boundStatement = preparedStatement.bind(recordid);
        ResultSet resultSet = session.execute(boundStatement);

        return "record deleted";
    }

    public String update(String keyspace, String recordid) {
        session = sessionMap.get(keyspace);
        List<Long> cursorDetailsList = new ArrayList<>();
        PreparedStatement preparedStatement;
        if (Statement.containsKey(keyspace + "select")) {
            preparedStatement = Statement.get(keyspace + "select");
        } else {
            preparedStatement = session.prepare(String.format("SELECT recordversion FROM %s.records where recordid=? ALLOW FILTERING", keyspace));
            Statement.put(keyspace + "select", preparedStatement);
        }
        BoundStatement boundStatement = preparedStatement.bind(recordid);
        ResultSet resultSet = session.execute(boundStatement);
        for (com.datastax.driver.core.Row row : resultSet) {
            cursorDetailsList.add(row.getLong("recordversion"));
        }
        Long[] versions = cursorDetailsList.toArray(new Long[0]);
        String s = Arrays.toString(versions);
        s.split(",");
        //  PreparedStatement statement = session.prepare("UPDATE stackoverflow2.alpha_screen " + "SET screen=? WHERE alpha=?");
        // for(int i=0;i<versions.length;i++) {
        if (Statement.containsKey(keyspace + "update")) {
            preparedStatement = Statement.get(keyspace + "update");
        } else {
            preparedStatement = session.prepare(String.format("UPDATE %s.records " + "SET techassurance=\'unevaluated\' where recordid=? and recordversion IN ?", keyspace));
            Statement.put(keyspace + "update", preparedStatement);
        }
        boundStatement = preparedStatement.bind(recordid, cursorDetailsList);
        // Bind the parameters and execute the statement
        resultSet = session.execute(boundStatement);
        //}

        return "record updated";
    }

    public void createSchema() {
        session = sessionMap.get("records");
        session.execute("CREATE KEYSPACE records_bk WITH replication "
                + "= {'class':'SimpleStrategy', 'replication_factor':3};");
        session.execute("CREATE TABLE records_bk.VARIATIONS (" + "RECORD_ID text,"
                + "RECORD_VERSION bigint," + "TECH_ASSURANCE text," + "PRIMARY KEY (RECORD_ID, RECORD_VERSION)" + ");");
    }


    public String getKeyspace() {
        String dataPartition = "records";
        dataPartition = dataPartition.replace('-', '_');
        String keyspace = dataPartition + "_" + "bk";
        return keyspace;
    }

    public void saveAll(List<Record> cursorDetailsForNextBatches, String keyspace) {
        System.out.println("saveAll - Saving total records " + cursorDetailsForNextBatches.size());
        try {
            session = sessionMap.get(keyspace);
            int size = cursorDetailsForNextBatches.size();
            PreparedStatement prepared = session.prepare(String.format("INSERT INTO %s.records (RECORDID,RECORDVERSION,TECHASSURANCE) VALUES ( ?, ?, ?)", "versionindexer"));
            //To avoid invalid batch size exception saving in batches of 100
            if (size > 100) {
                for (int i = 0; i < size; i += 100) {
                    BatchStatement batch = new BatchStatement();
                    List<Record> batchForSearch = cursorDetailsForNextBatches.subList(i, Math.min(i + 100, size));
                    for (Record details : batchForSearch) {
                        batch.add(prepared.bind(details.getRecordId(), details.getVersion(), details.getTechnicalAssurance()));
                    }
                    session.execute(batch);
                }
            } else {
                BatchStatement batch = new BatchStatement();
                for (Record details : cursorDetailsForNextBatches) {
                    batch.add(prepared.bind(details.getRecordId(), details.getVersion(), details.getTechnicalAssurance()));

                }
                session.execute(batch);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }

}
