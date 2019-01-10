package second.logannalsis;

public class LogModel {

    private  String  ipAddress;
    private  String  clientIndentId;
    private  String userId;
    private  String dateTime;
    private  String method;
    private  String url;
    private  String proTocal;
    private  Integer responseCode;
    private  Long contentSize;

    public LogModel(String ipAddress, String clientIndentId, String userId, String dateTime, String method, String url, String proTocal, Integer responseCode, Long contentSize) {
        this.ipAddress = ipAddress;
        this.clientIndentId = clientIndentId;
        this.userId = userId;
        this.dateTime = dateTime;
        this.method = method;
        this.url = url;
        this.proTocal = proTocal;
        this.responseCode = responseCode;
        this.contentSize = contentSize;
    }

    public LogModel() {
    }

    @Override
    public String toString() {
        return "LogModel{" +
                "ipAddress='" + ipAddress + '\'' +
                ", clientIndentId='" + clientIndentId + '\'' +
                ", userId='" + userId + '\'' +
                ", dateTime='" + dateTime + '\'' +
                ", method='" + method + '\'' +
                ", url='" + url + '\'' +
                ", proTocal='" + proTocal + '\'' +
                ", responseCode=" + responseCode +
                ", contentSize=" + contentSize +
                '}';
    }
}
