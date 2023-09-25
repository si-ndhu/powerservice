class Point{
     private class pair {
        int x;
        int y;
    }
//    String hubIdentifier;
    int xCoordinate;
    int yCoordinate;
    Point() {
        //this.hubIdentifier=hubIdentifier;
    }
    public pair getCoordinates() {
        pair p=new pair();
        p.x=xCoordinate;
        p.y=yCoordinate;
        return p;
    }
    public void setCoordinates(int xCoordinate,int yCoordinate) {
        this.xCoordinate = xCoordinate;
        this.yCoordinate=yCoordinate;
    }
}