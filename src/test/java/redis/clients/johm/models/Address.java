package redis.clients.johm.models;

import redis.clients.johm.Attribute;
import redis.clients.johm.Id;
import redis.clients.johm.Indexed;
import redis.clients.johm.Model;

@Model
public class Address {
    @Id
    @TestAnnotation
    private Long id;
    @Attribute
    @Indexed
    private String streetName;

    public Long getId() {
        return id;
    }

    public String getStreetName() {
        return streetName;
    }

    public void setStreetName(String streetName) {
        this.streetName = streetName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((streetName == null) ? 0 : streetName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Address)) {
            return false;
        }
        Address other = (Address) obj;
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        if (streetName == null) {
            if (other.streetName != null) {
                return false;
            }
        } else if (!streetName.equals(other.streetName)) {
            return false;
        }
        return true;
    }
}
