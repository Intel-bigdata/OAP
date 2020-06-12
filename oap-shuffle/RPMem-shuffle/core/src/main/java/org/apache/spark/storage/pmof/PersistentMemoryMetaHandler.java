package org.apache.spark.storage.pmof;
 
import java.nio.channels.FileLock;
import java.sql.Connection;
import org.sqlite.SQLiteConfig;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.io.*;

public class PersistentMemoryMetaHandler {

  private final String url;
  private final String fileLockPath;
  private final File file;

  PersistentMemoryMetaHandler(String root_dir) {
    url = "jdbc:sqlite:" + root_dir + "/spark_shuffle_meta.db";
    fileLockPath = root_dir + "spark_shuffle_file.lock";
    file = new File(fileLockPath);
    createTable(root_dir);
  }

  public void createTable(String root_dir) {
    String sql = "CREATE TABLE IF NOT EXISTS metastore (\n"
                + "	shuffleId text PRIMARY KEY,\n"
                + "	device text NOT NULL,\n"
                + " UNIQUE(shuffleId, device)\n"
                + ");\n";

    FileOutputStream fos = null;
    FileLock fl = null;
    Connection conn = null;
    Statement stmt = null;
    try {
      fos = new FileOutputStream(file);
      fl = fos.getChannel().lock();
      conn = DriverManager.getConnection(url);
      stmt = conn.createStatement();
      stmt.execute(sql);

      sql = "CREATE TABLE IF NOT EXISTS devices (\n"
              + "	device text UNIQUE,\n"
              + "	mount_count int\n"
              + ");";
      stmt.execute(sql);
    } catch (SQLException e) {
      System.out.println("createTable failed:" + e.getMessage());
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (stmt != null) stmt.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      try {
        if (conn != null) conn.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      try {
        if (fl != null) {
          fl.release();
        }
        if (fos != null) {
          fos.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    System.out.println("Metastore DB connected: " + url);
  }

  public void insertRecord(String shuffleId, String device) {
    String sql = "INSERT OR IGNORE INTO metastore(shuffleId,device) VALUES('" + shuffleId + "','" + device + "')";
    FileOutputStream fos = null;
    FileLock fl = null;
    Connection conn = null;
    Statement stmt = null;
    try {
      fos = new FileOutputStream(file);
      fl = fos.getChannel().lock();
      SQLiteConfig config = new SQLiteConfig();
      config.setBusyTimeout("30000");
      conn = DriverManager.getConnection(url);
      stmt = conn.createStatement();
      stmt.executeUpdate(sql);
    } catch (SQLException e) {
      e.printStackTrace();
      System.exit(-1);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (stmt != null) stmt.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      try {
        if (conn != null) conn.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      try {
        if (fl != null) {
          fl.release();
        }
        if (fos != null) {
          fos.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public String getShuffleDevice(String shuffleId){
    String sql = "SELECT device FROM metastore where shuffleId = ?";
    String res = "";
    FileOutputStream fos = null;
    FileLock fl = null;
    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      fos = new FileOutputStream(file);
      fl = fos.getChannel().lock();
      conn = DriverManager.getConnection(url);
      stmt = conn.prepareStatement(sql);
      stmt.setString(1, shuffleId);
      rs = stmt.executeQuery();
      while (rs.next()) {
        res = rs.getString("device");
      }
    } catch (SQLException e) {
      e.printStackTrace();
      System.exit(-1);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (rs != null) rs.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      try {
        if (stmt != null) stmt.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      try {
        if (conn != null) conn.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      try {
        if (fl != null) {
          fl.release();
        }
        if (fos != null) {
          fos.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return res;
  }

  public String getUnusedDevice(ArrayList<String> full_device_list){
    String sql = "SELECT device, mount_count FROM devices";
    ArrayList<String> device_list = new ArrayList<String>();
    HashMap<String, Integer> device_count = new HashMap<String, Integer>();
    String device = "";
    int count;
    FileOutputStream fos = null;
    FileLock fl = null;
    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      fos = new FileOutputStream(file);
      fl = fos.getChannel().lock();
      SQLiteConfig config = new SQLiteConfig();
      config.setBusyTimeout("30000");
      conn = DriverManager.getConnection(url);
      stmt = conn.createStatement();
      rs = stmt.executeQuery(sql);
      while (rs.next()) {
        device_list.add(rs.getString("device"));
        device_count.put(rs.getString("device"), rs.getInt("mount_count"));
      }
      full_device_list.removeAll(device_list);
      if (full_device_list.size() == 0) {
        // reuse old device, picked the device has smallest mount_count
        device = getDeviceWithMinCount(device_count);
        if (device != null && device.length() == 0) {
          throw new SQLException();
        }
        count = (Integer) device_count.get(device) + 1;
        sql = "UPDATE devices SET mount_count = " + count + " WHERE device = '" + device + "'\n";
      } else {
        device = full_device_list.get(0);
        count = 1;
        sql = "INSERT OR IGNORE INTO devices(device, mount_count) VALUES('" + device + "', " + count + ")\n";
      }

      System.out.println(sql);

      stmt.executeUpdate(sql);
    } catch (SQLException e) {
      e.printStackTrace();
      System.exit(-1);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (rs != null) rs.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      try {
        if (stmt != null) stmt.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      try {
        if (conn != null) conn.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      try {
        if (fl != null) {
          fl.release();
        }
        if (fos != null) {
          fos.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    System.out.println("Metastore DB: get unused device, should be " + device + ".");
    return device;
  }

  public void remove() {
    new File(url).delete();
  }

  private String getDeviceWithMinCount(HashMap<String, Integer> device_count_map) {
    String device = "";
    int count = -1;
    for (Map.Entry<String, Integer> entry : device_count_map.entrySet()) {
      if (count == -1 || entry.getValue() < count) {
        device = entry.getKey();
        count = entry.getValue();
      }
    }
    return device;
  }
}
