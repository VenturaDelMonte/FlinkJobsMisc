package de.adrianbartnik.benchmarks.nexmark;


import java.io.Serializable;

/**
 * Schema: timestamp,person_id,name,email_address,phone,street,city,country,province,zipcode,homepage,creditcard
 */
public class NewPersonEvent implements Serializable {

    private final long timestamp;
    private final int personId;
    private final String name;
    private final String email;
    private final String city;
    private final String country;
    private final String province;
    private final String zipcode;
    private final String homepage;
    private final String creditcard;

    public NewPersonEvent(long timestamp,
                          int personId,
                          String name,
                          String email,
                          String city,
                          String country,
                          String province,
                          String zipcode,
                          String homepage,
                          String creditcard) {
        this.timestamp = timestamp;
        this.personId = personId;
        this.email = email;
        this.creditcard = creditcard;
        this.city = city;
        this.name = name;
        this.country = country;
        this.province = province;
        this.zipcode = zipcode;
        this.homepage = homepage;
    }

    public String getName() {
        return name;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Integer getPersonId() {
        return personId;
    }

    public String getEmail() {
        return email;
    }

    public String getCreditcard() {
        return creditcard;
    }

    public String getCity() {
        return city;
    }

    public String getCountry() {
        return country;
    }

    public String getProvince() {
        return province;
    }

    public String getZipcode() {
        return zipcode;
    }

    public String getHomepage() {
        return homepage;
    }
}
