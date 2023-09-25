import java.util.*;
import java.sql.*;
import java.io.*;

class DamagedPostalCodes{
    String postalCode;
    float repairEstimate;//
    public String getPostalCode() {
        return postalCode;
    }
    public float getRepairEstimate() {
        return repairEstimate;
    }

    public void setPostalCode(String postalCode) {
        this.postalCode = postalCode;
    }
    public void setRepairEstimate(int repairEstimate) {
        this.repairEstimate = repairEstimate;
    }
}
