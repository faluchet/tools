package rucio.dumps;

import java.io.IOException;
import java.lang.StringBuffer;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Calendar;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;

import java.net.InetAddress;

/**
 * Servlet implementation class ReadFromHdfs
 */
public class GetFileFromHDFS extends HttpServlet {
    private static final long serialVersionUID = 1L;

    /**
     * @see HttpServlet#HttpServlet()
     */
    public GetFileFromHDFS() {
        super();
        // TODO Auto-generated constructor stub
    }

    /**
     * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
     *      response)
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        if (ugi.isFromKeytab()) {
            ugi.checkTGTAndReloginFromKeytab(); // just in case, even the refresher thread might have run out
        } else if (!ugi.hasKerberosCredentials()) {
            String user = "tomcat/" + InetAddress.getLocalHost().getHostName() + "@CERN.CH";
            UserGroupInformation.loginUserFromKeytab(user, "/etc/hadoop/conf/tomcat.keytab");
            ugi = UserGroupInformation.getCurrentUser();
        }

        String date_param = request.getParameter("date");

        String path = null;
        String date = null;
        if (date_param != null) {
            try {
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                Date tmpDate = formatter.parse(date_param);
                formatter = new SimpleDateFormat("yyyy-MM-dd");
                String tmpStrDate = formatter.format(tmpDate);
                String tmpPath = generatePath(tmpStrDate);
                if (checkPath(tmpPath)) {
                    date = tmpStrDate;
                    path = tmpPath;
                }
            } catch (ParseException e) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                        "Your date parameter is malformed. The expected format is \"YYYY-MM-DD\"");
                return;
            }
        } else {
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Calendar cal = Calendar.getInstance();
            String tmpDate = dateFormat.format(cal.getTime());
            for (int i = 0; i < 14; i++) {
                String tmpPath = generatePath(tmpDate);
                System.out.println(tmpPath);
                if (checkPath(tmpPath)) {
                    date = tmpDate;
                    path = tmpPath;
                    break;
                }
                cal.add(Calendar.DATE, -1);
                tmpDate = dateFormat.format(cal.getTime());
            }
        }

        if (path == null && date_param == null) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND,
                    "Could not find any lost files report in the last 2 weeks");
            return;
        } else if (path == null && date_param != null) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND,
                    "Could not find any lost files report for date " + date_param);
            return;
        }

        response.setContentType("application/text");
        response.setHeader("Content-Disposition", "attachment;filename=" + "transfers_" + date + ".csv");

        ServletOutputStream output = response.getOutputStream();
        FileSystem fs = DistributedFileSystem.get(new Configuration());
        Path p = new Path(path);
        FileStatus[] status = fs.listStatus(p);
        for (int i = 0; i < status.length; i++) {
            // System.out.println(status[i]);
            FSDataInputStream in = fs.open(status[i].getPath());

            byte b[] = new byte[4096];
            int len;

            while ((len = in.read(b)) > 0) {
                if (len < 4096) {
                    output.write(Arrays.copyOfRange(b, 0, len));
                } else {
                    output.write(b);
                }
            }
            in.close();
        }
    }

    /**
     * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
     *      response)
     */
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        // TODO Auto-generated method stub
    }

    private String generatePath(String date) {
        StringBuffer path = new StringBuffer("/user/rucio01/reports/");
        path.append(date);
        path.append("/suspicious_files.csv/");
        System.out.println(path);
        return path.toString();
    }

    private boolean checkPath(String path) {
        try {
            FileSystem fs = DistributedFileSystem.get(new Configuration());
            Path p = new Path(path);
            FileStatus[] status = fs.listStatus(p);
            if (status.length > 0) {
                return true;
            }
        } catch (IOException e) {
            return false;
        }
        return false;
    }

}
