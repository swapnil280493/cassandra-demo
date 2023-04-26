package com.example.cassandraDemo.controller;

import com.datastax.driver.core.Session;
import com.example.cassandraDemo.dao.CassandraConnectionUtil;
import com.example.cassandraDemo.dao.RecordDao;
import com.example.cassandraDemo.model.Record;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
@RestController
public class RecordController {
    @Autowired
    private RecordDao rd;
    @Autowired
    CassandraConnectionUtil cassandraConnectionUtil;
    //extends CassandraRepository<SearchDetails, String> {
    Session session = null;

    @PostMapping("/saveRecords")
    public void saveRecord() {

        List<Record> RL = new ArrayList<>();
       /* JsonObject data =new JsonObject();
        data.add("TechnicalAssuranceTypeID",new JsonParser().parse("petronas-osdu:reference-data--TechnicalAssuranceType:Suitable:"));

        String TA = null;
        if(data.has("TechnicalAssuranceTypeID")){
            JsonElement jsonElement= data.get("TechnicalAssuranceTypeID");

            TA = jsonElement.getAsString().split(":")[2];
        }

        System.out.println(TA);*/

        RL.add(new Record("r1", 1, "sample "));
        RL.add(new Record("r1", 2, "Suitable"));
        RL.add(new Record("r1", 3, "authorised"));
        RL.add(new Record("r1", 4, "Certified"));
        rd.saveAll(RL, "versionindexer");

    }

    @GetMapping("/getKeyspace")
    public String getKeyspace() {
        return rd.getKeyspace();
    }

    @PostMapping("/createSchema")
    public void createSchema() {
        rd.createSchema();
    }

    /* @GetMapping("/getRecord")
        public  Record getRecord(@RequestParam(value = "recordId") String recordid, @RequestParam(value = "version") int version){

            List<Record>RL= new ArrayList<>();
            JsonObject data =new JsonObject();
            data.addProperty("TechnicalAssuranceTypeID","petronas-osdu:reference-data--TechnicalAssuranceType:Suitable:");

            //data.add("TechnicalAssuranceTypeID",new Ja("petronas-osdu:reference-data--TechnicalAssuranceType:Suitable:"));

            String TA = null;
            if(data.has("TechnicalAssuranceTypeID")){
                JsonElement jsonElement= data.get("TechnicalAssuranceTypeID");

                TA = jsonElement.getAsString().split(":")[2];
            }

            System.out.println(TA);
            return rd.findByRecordIdAndVersion(recordid,version);
        }
    */
    @GetMapping("/getAllRecords")
    public List<Record> getAllRecords(@RequestParam(name = "keyspace") String keyspace) {
        return (List<Record>) rd.findAll(keyspace);
    }

    @GetMapping("/deleteRecord")
    public String deleteRecord(@RequestParam(name = "keyspace") String keyspace, @RequestParam(name = "recordid") String recordid) {

        return rd.delete(keyspace,recordid);
    }
    @GetMapping("/updateRecord")
    public String updateRecord(@RequestParam(name = "keyspace") String keyspace, @RequestParam(name = "recordid") String recordid) {

        return rd.update(keyspace,recordid);
    }

    /*@PostMapping("/updateRecord")
    public void setRecord(){
        Record r1= new Record("r1",2,"trusted");
        rd.save(r1);
    }*/
}
