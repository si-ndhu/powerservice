import java.util.*;
import java.sql.*;
import java.io.*;

public class PowerService {
    Map<String,Integer> peopleInHub=new HashMap<>(); //hubid, no. of people covered by hub
    Map<String,Float> hubsByPeople=new HashMap<>(); //postalcode, avg hubs by people
    Map<String,Float> hubsByArea=new HashMap<>(); //postalcode, avg hubs by area
    Map<String,Integer> peopleInPostal=new HashMap<>();//postal code, people living in that postal code
    Map<String,Integer> eachHubPeople= new HashMap<>();//hub id, people covered by that hubid
    Map<String,Integer> hubCntforPostal=new HashMap<>();//postal code, no. of hubs
    Map<String,Float> hubRepairtime=new HashMap<>(); //hubid, estimated time needed to fix the hub
    Map<String,Set<String>> hubforPostals=new HashMap<>();//postal code, hubs covering that postal code
    Map<String, Float> postalRepairEstimate = new HashMap<>();//postalcode, estimate time needed to repair that postalcode hubs
    Map<String,Float> significantTime= new HashMap<>();//hubid, total impact value of that hub
    Map<String,Float> hubtotalRepairtime=new HashMap<>(); //hubid, repair time needed by the hub in total including estimated time

    Connection con=null;
    Statement statement=null;
    ResultSet resultSet=null;
    PowerService() {
        DbConnection db = new DbConnection();
        con = db.makeConnection();
    }
    private class pair{
        int x;
        int y;
    }

    /**
     * This function is to assign needed data to maps to implement reporting and planning methods.It also updates the existing tables column values
     */
    void createMaps(){
        String query="select postalcode,population from postalcode";
        try {
            statement = con.createStatement();
            resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                peopleInPostal.put(resultSet.getString("postalcode"), resultSet.getInt("population"));
            }
            query="select ServicedArea,count(*) as hubcount from distribHub group by ServicedArea";
            resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                hubCntforPostal.put(resultSet.getString("ServicedArea"), resultSet.getInt("hubcount"));
            }
            for (String postal : hubCntforPostal.keySet()) {
                query = "update distribHub set population=" + (peopleInPostal.get(postal))/(hubCntforPostal.get(postal)) + " where ServicedArea=" + "\"" + postal + "\"";
                statement.execute(query);
            }
            for (String postal : hubCntforPostal.keySet()) {
                query = "update postalcode set hubcnt=" + hubCntforPostal.get(postal)+ " where postalcode=" + "\"" + postal + "\"";
                statement.execute(query);
            }
            query="select postalcode,(hubcnt/population) as hubBypopulation from postalcode";
            resultSet=statement.executeQuery(query);
            while(resultSet.next()){
                hubsByPeople.put(resultSet.getString("postalcode"), resultSet.getFloat("hubByPopulation"));
            }
            query="select postalcode,(hubcnt/area) as hubByarea from postalcode";
            resultSet=statement.executeQuery(query);
            while(resultSet.next()){
                hubsByArea.put(resultSet.getString("postalcode"), resultSet.getFloat("hubByarea"));
            }
            query="select hubid, repairEstimate from hubDamage";
            resultSet=statement.executeQuery(query);
            while(resultSet.next()){
                hubRepairtime.put(resultSet.getString("hubid"), resultSet.getFloat("repairEstimate"));
            }
            query="select hubid,sum(population) as totalpeople from distribHub group by hubid";
            resultSet=statement.executeQuery(query);
            while(resultSet.next()){
                eachHubPeople.put(resultSet.getString("hubid"), resultSet.getInt("totalpeople"));
            }

             query = "select servicedArea,hubid from distribHub order by ServicedArea";
             resultSet = statement.executeQuery(query);
            String hubs = "",postal,hubid;
            Set<String> newset,set,tempset;
            //create hubforPostals map
            while (resultSet.next()) {
                set=new HashSet<>();
                 postal=resultSet.getString("servicedArea");
                 hubid=resultSet.getString("hubid");
                if(hubforPostals.containsKey(postal)) {
                    newset=new HashSet<>();
                    newset.addAll(hubforPostals.get(postal));
                    newset.add(hubid);
                    hubforPostals.put(postal,newset);
                }
                else {
                    set.add(resultSet.getString("hubid"));
                    hubforPostals.put(postal,set);
                }
            }
            float time;
            for (String postalId : hubforPostals.keySet()) {
                time=0;
                tempset = new HashSet<>();
                tempset = hubforPostals.get(postalId);
                Iterator<String> itr = tempset.iterator();
                while (itr.hasNext()) {
                    String hub = itr.next();
                    query = "select repairEstimate from hubDamage where hubID=" + "\"" + hub + "\"";
                    resultSet = statement.executeQuery(query);
                    resultSet.next();
                    time = time + resultSet.getFloat("repairEstimate");
                }
                postalRepairEstimate.put(postalId, time);
            }
            for(String area: postalRepairEstimate.keySet()) {
                query = "update postalcode set repairTime=" +postalRepairEstimate.get(area)+"where postalcode=" +"\""+ area+"\"";
                statement.execute(query);
            }

            query="select sum(repairTime) as totalrepairtime,HubId from hubRepair group by HubId";
            resultSet = statement.executeQuery(query);
            String hub;
            //create hubRepairtime map
            while (resultSet.next()) {
                hub=resultSet.getString("hubid");
                time=resultSet.getFloat("totalrepairtime");
                hubtotalRepairtime.put(hub,time);
            }
            for(String eachHub:hubtotalRepairtime.keySet()) {
                query = "update hubDamage set totaltime=" + (hubtotalRepairtime.get(eachHub) + hubRepairtime.get(eachHub)) + " where hubid=" + "\"" + eachHub + "\"";
                statement.execute(query);
            }
            String Hub="";
            float repairtime,impactvalue=0;
            query= "select Hubid, totaltime from hubDamage";
            resultSet=statement.executeQuery(query);
            while(resultSet.next()) {
                Hub = resultSet.getString("hubid");
                repairtime = resultSet.getFloat("totaltime");
                impactvalue = (int)(eachHubPeople.get(Hub) / repairtime);
                significantTime.put(Hub,impactvalue);
            }
            for(String temphub:significantTime.keySet()){
                query="update hubDamage set totaltime="+significantTime.get(temphub)+ " where hubid="+"\""+temphub+"\"";
                statement.execute(query);
            }
        }
        catch (Exception e){
            System.out.println("connection failed "+ e);
        }
    }
    /**
     *
     * @param postalCode - postal code identifier of the area
     * @param population - no. of people living in that area
     * @param area - area in square metres covered by the postalcode.
     * @return - returns true when postal code is added to the system.
     */
    boolean addPostalCode(String postalCode, int population, int area) {
        if(postalCode==null||postalCode==""||population<=0||area<=0)
            return false;
        String query="insert into postalcode(postalcode,population,area) values("+"\""+postalCode+"\""+","+population+","+area+")";
            try {
                statement = con.createStatement();
                statement.execute(query);
                statement.close();
            }
            catch (Exception e){
                System.out.println("connection failed "+ e);
            }
        return true;
    }
    /**
     *
     * @param hubIdentifier - unique identifier name of the hub
     * @param location - location at which the hub is located
     * @param servicedAreas - postal codes/areas covered by the hub
     * @return - returns true if hub is added to the system
     */
    boolean addDistributionHub ( String hubIdentifier, Point location, Set<String> servicedAreas ) {
        if(hubIdentifier==null||hubIdentifier==""||location==null||servicedAreas.isEmpty()) {
            return false;
        }
        int  x=location.xCoordinate;
        int y=location.yCoordinate;
        String key="";
        String loc="\"("+x+","+y+")\"";
        Iterator<String> postalcodeitr=servicedAreas.iterator();
        while(postalcodeitr.hasNext()) {
             key=postalcodeitr.next();
            String query="insert into distribHub(Hubid,location,servicedarea) values("+"\""+hubIdentifier+"\""+","+loc+","+"\""+key+"\""+")";
            try {
                statement = con.createStatement();
                statement.execute(query);
                statement.close();
            }
            catch (Exception e){
                System.out.println("connection failed "+ e);
            }
        }
        return  true;
    }

    /**
     *
     * @param hubIdentifier- name of the hub
     * @param repairEstimate - estimate no of hours to repair the hub
     */
    void hubDamage ( String hubIdentifier, float repairEstimate ){
        if(hubIdentifier==null||hubIdentifier==""||repairEstimate<=0)
            return;
        String query="insert into hubDamage(hubId,repairEstimate) values("+"\""+hubIdentifier+"\""+","+repairEstimate+")";
        try {
            statement = con.createStatement();
            statement.execute(query);
            statement.close();
        }
        catch (Exception e){
            System.out.println("query failed "+ e);
        }
    }

    /**
     *
     * @param hubIdentifier - Unique identifier/name for the hub
     * @param employeeId- Id of the employee who worked on the hub
     * @param repairTime - Time spent by the employee working on fixing the hub
     * @param inService - indicator to tell if hub is in service or not
     */
    void hubRepair( String hubIdentifier, String employeeId, float repairTime, boolean inService ){
        if(hubIdentifier==null||hubIdentifier==""||employeeId==""||employeeId==null||repairTime<=0)
            return;
        String query="insert into hubRepair values("+"\""+hubIdentifier+"\""+","+"\""+employeeId+"\""+","+repairTime+","+inService+")";
        try {
            statement = con.createStatement();
            statement.execute(query);
            resultSet= statement.executeQuery("select repairEstimate from hubDamage where HubId='"+hubIdentifier+"'");
            resultSet.next();
            float repairEstimate= resultSet.getFloat("repairEstimate");
            if(inService==true) {
                statement.execute("update hubDamage set repairEstimate="+0+" where HubId='"+hubIdentifier+"'");
            }
            else if(inService==false&&repairTime<repairEstimate) {//update the repair estimate after work started
                statement.execute("update hubDamage set repairEstimate="+(repairEstimate-repairTime)+" where HubId='"+hubIdentifier+"'");
            }
            statement.close();
        }
        catch (Exception e){
            System.out.println("connection failed "+ e);
        }
    }

    /**
     *
     * @return- returns no. of people who are out of service in total in all of the hubs
     */
    int peopleOutOfService () {
//        createMaps();
        float noofpeople=0;
        int people=0;
        Set <String> hubsNotinService=new HashSet<>();
        String query,hub;
        query = "select hubid from hubRepair EXCEPT select hubid from hubRepair where inService=true";
        List<String> postalcodes;
        try {
            resultSet = statement.executeQuery(query);
            while(resultSet.next()){
                hubsNotinService.add(resultSet.getString("hubid"));
            }
            Iterator<String> itr=hubsNotinService.iterator();
            while(itr.hasNext()) {
                 hub=itr.next();
                 query="select sum(population) as peoplecnt from distribHub where hubId="+"\""+hub+"\""+ " group by hubid" ;
                resultSet = statement.executeQuery(query);
                resultSet.next();
                noofpeople=noofpeople+resultSet.getFloat("peoplecnt");
            }
        } catch (Exception e) {
            System.out.println("connection failed " + e);
        }
        return (int)noofpeople;
    }

    /**
     *
     * @param limit- integer parameter to limit the no. of most damaged postal code rows
     * @return - returns the list of most damaged postal codes in order
     */
    List<DamagedPostalCodes> mostDamagedPostalCodes ( int limit ) {
        if(limit<=0)
            return null;
        DamagedPostalCodes d;
        List<DamagedPostalCodes> returnlist=new ArrayList<>();
        String query;
        try {
            statement = con.createStatement();
            query= "select postalcode, repairTime from postalcode order by repairTime desc limit "+ limit;
            resultSet=statement.executeQuery(query);
            while(resultSet.next()){
                 d=new DamagedPostalCodes();
                d.repairEstimate=resultSet.getFloat("repairTime");
                d.postalCode=resultSet.getString("postalcode");
                returnlist.add(d);
            }
        }
        catch (Exception e) {
            System.out.println("connection failed " + e);
        }
        return returnlist;
    }

    /**
     *
     * @param limit- to limit the output rows of most significant hubs
     * @return - returns the list of most significant hubs in order
     */
    List<HubImpact> fixOrder ( int limit ){
        if(limit<=0)
            return null;
        List<HubImpact> returnlist=new ArrayList<>();
        List<HubImpact> hubImpactList=new ArrayList<>();
        String Hubid="";
        float time,repairtime,significantHubtime=0;
        String query="";
        try {
            statement = con.createStatement();
            query= "select Hubid, totaltime from hubDamage order by totaltime desc limit "+ limit;
            resultSet=statement.executeQuery(query);
            while(resultSet.next()) {
                Hubid = resultSet.getString("hubid");
                repairtime = resultSet.getFloat("totaltime");
                HubImpact h=new HubImpact();
                h.hubId=Hubid;
                h.impact=repairtime;
                hubImpactList.add(h);
            }
        }
        catch (Exception e) {
            System.out.println("connection failed " + e);
        }
        return hubImpactList;
    }

    /**
     *
     * @param increment- rate at which the service restroration is computed.
     * @return - list of values to indicate the hours in which the service can be restored.
     */
    List<Integer> rateOfServiceRestoration ( float increment ){
        List<Integer> hourlist=new ArrayList<>();
        hourlist.add(0);
        String query="select population from postalcode";
        int totalpeople=0;
        int peopleinservice=0;
        try {
            statement = con.createStatement();
            resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                totalpeople = totalpeople + resultSet.getInt("population");
            }
            int peopleoutofservice = peopleOutOfService();
            peopleinservice = totalpeople - peopleoutofservice;
            query = "select Hubid, totaltime from hubDamage order by totaltime desc";
            resultSet = statement.executeQuery(query);
            float time = 0;
            float i;
            for (i = increment; i <= 1; i = i + increment) {
                if ((i * totalpeople) <= peopleinservice) {
                    hourlist.add(0);
                    continue;
                }
                resultSet.next();
                String hub = resultSet.getString("hubid");
                time = time + hubtotalRepairtime.get(hub);
                while ((i<=1)&&(i * totalpeople <= (peopleinservice + eachHubPeople.get(hub)))) {
                    hourlist.add((int) time);
                    i=i+increment;
                }
                if(i<=1) {
                    i=i-increment;
                    continue;
                }
                else
                    break;
            }
        }
        catch (Exception e) {
            System.out.println("connection failed " + e);
        }

        return hourlist;
    }

    /**
     *
     * @param limit - to limit the output no. of rows to return
     * @return- list of underserved postalcodes by comparing no. of people living
     */
    List<String> underservedPostalByPopulation ( int limit ) {
        if(limit<=0)
            return null;
        List<String> returnlist=new ArrayList<>();
        String query="select count(*) from postalBypopulation";
        try {
            if(!statement.execute(query)) {
                for (String postal : hubsByPeople.keySet()) {
                    query = "insert into postalBypopulation values(" + "\"" + postal + "\"," + hubsByPeople.get(postal) + ")";
                    statement = con.createStatement();
                    statement.execute(query);
                }
            }
            query="select postalcode,hubsbypopulation from postalBypopulation order by hubsbypopulation limit "+limit;
            resultSet = statement.executeQuery(query);
            while(resultSet.next()) {
                returnlist.add(resultSet.getString("postalcode"));
            }
        }
        catch (Exception e) {
            System.out.println("connection failed " + e);
        }
        return returnlist;
    }

    /**
     *
     * @param limit- to output limit no. of underserved postal codes.
     * @return- list of underserved postal comparing with area covered
     */
    List<String> underservedPostalByArea ( int limit ){
        if(limit<=0)
            return null;
        List<String> returnlist=new ArrayList<>();
        String query="select count(*) from postalByarea";
        try {
            if(!statement.execute(query)) {
                for (String postal : hubsByArea.keySet()) {
                    query = "insert into postalByarea values(" + "\"" + postal + "\"," + hubsByArea.get(postal) + ")";
                    statement = con.createStatement();
                    statement.execute(query);
                }
            }

            query="select postalcode,hubsbyarea from postalByarea order by hubsbyarea limit "+limit;
            resultSet = statement.executeQuery(query);
            while(resultSet.next()) {
                returnlist.add(resultSet.getString("postalcode"));
            }
        }
        catch (Exception e) {
            System.out.println("connection failed " + e);
        }
            return returnlist;
    }

    public static void main(String[] args) {
        PowerService p1=new PowerService();
//        p1.addPostalCode("B3J",3000, 50);
//        p1.addPostalCode("B0C",5000, 100);
//        p1.addPostalCode("B1W",7000, 75);
//        p1.addPostalCode("B0E",1000, 125);
//        p1.addPostalCode("B3L",2500, 200);
//        p1.addPostalCode("B3K",6000, 500);
//
//        Set s1 =new HashSet<String>();
//        s1.add("B3J");
//        s1.add("B3L");
//        s1.add("B3K");
//        Point p=new Point();
//        p.setCoordinates(10,20);
//        p1.addDistributionHub("H1",p,s1);
//        p.setCoordinates(5,7);
//        Set s2=new HashSet<String>();
//        s2.add("B0C");
//        s2.add("B3L");
//        p1.addDistributionHub("H2",p,s2);
//        p.setCoordinates(15,30);
//        Set s3=new HashSet<String>();
//        s3.add("B1W");
//        s3.add("B0E");
//        p1.addDistributionHub("H3",p,s3);
//
//        p1.hubDamage("H1",10.5f);
//        p1.hubDamage("H2",5);
//        p1.hubDamage("H3",7.2f);
//
//        p1.hubRepair("H1","E1", 3,false);
//        p1.hubRepair("H2","E2",5.4f,false);
//        p1.hubRepair("H3","E3", 4,false);
//        p1.hubRepair("H1","E4",7.5f,true);
//        p1.hubRepair("H2","E5",3,false);
//        p1.hubRepair("H2","E6",2.8f,true);

        p1.createMaps();

        System.out.println(p1.underservedPostalByArea(4));
        System.out.println(p1.underservedPostalByPopulation(5));
        System.out.println(p1.peopleOutOfService());
        List<DamagedPostalCodes> l= p1.mostDamagedPostalCodes(3);
        for(int i=0;i<l.size();i++) {
            System.out.println(l.get(i).postalCode+" "+l.get(i).repairEstimate);
        }
        List<HubImpact> l2=p1.fixOrder(3);
        for(int i=0;i<l2.size();i++) {
            System.out.println(l2.get(i).hubId+" "+l2.get(i).impact);
        }
        List<Integer> l3=p1.rateOfServiceRestoration(0.05f);
        for(int i=0;i<l3.size();i++) {
        System.out.println(l3.get(i));
        }
    }
}