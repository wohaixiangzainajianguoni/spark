package sparksql;

public class LogUtil {

//   10.0.0.153  # - # - #[12/Mar/2004:12:23:18-0800]#"GET /cgi-bin/mailgraph.cgi/mailgraph_3_err.png HTTP/1.1"#200#5554
//         0            1      2                  3                                                     4                                                                                   5       6

  public  static LogModel parseLog(String line)
    {
        System.out.println(line);
        String[] split = line.split("#");

//        for (String s:split)
//        {
//            System.out.println(s);
//        }


//        this.ipAddress = ipAddress;   0
//        this.clientIndentId = clientIndentId;  1
//        this.userId = userId; 2
//        this.dateTime = dateTime;  3
//        this.method = method;  4
//        this.url = url;  5
//        this.proTocal = proTocal;  6
//        this.responseCode = responseCode;   7
//        this.contentSize = contentSize;   8

        String substring = split[4].substring(1);
        String substring1 = substring.substring(0, substring.length() - 1);
        String[] s = substring1.split(" ");
        LogModel logModel = new LogModel(
                split[0],
                split[1],
                split[2],
                split[3],
                s[0],
                s[1],
                s[2],
                Integer.parseInt(split[5]),
                Long.parseLong(split[6])
        );
        return  logModel;
    }
    public static void main(String[] args) {

      String data="10.0.0.153#-#-#[12/Mar/2004:12:23:18-0800]#\"GET /cgi-bin/mailgraph.cgi/mailgraph_3_err.png HTTP/1.1\"#200#5554";
        LogModel logModel = parseLog(data);
        System.out.println(logModel);

    }
}
