package sparksql;

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


    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getClientIndentId() {
        return clientIndentId;
    }

    public void setClientIndentId(String clientIndentId) {
        this.clientIndentId = clientIndentId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getProTocal() {
        return proTocal;
    }

    public void setProTocal(String proTocal) {
        this.proTocal = proTocal;
    }

    public Integer getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(Integer responseCode) {
        this.responseCode = responseCode;
    }

    public Long getContentSize() {
        return contentSize;
    }

    public void setContentSize(Long contentSize) {
        this.contentSize = contentSize;
    }
}
