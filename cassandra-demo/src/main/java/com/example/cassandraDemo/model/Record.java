package com.example.cassandraDemo.model;

public class Record {
    private String recordId;

    private Long version;
    String technicalAssurance;

    public Record(String recordId, long version, String technicalAssurance) {
        this.recordId = recordId;
        this.version = version;
        this.technicalAssurance = technicalAssurance;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public String getRecordId() {
        return recordId;
    }

    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }

    public String getTechnicalAssurance() {
        return technicalAssurance;
    }


    public void setTechnicalAssurance(String technicalAssurance) {
        this.technicalAssurance = technicalAssurance;
    }


}
