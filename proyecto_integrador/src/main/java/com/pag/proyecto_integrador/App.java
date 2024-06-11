package com.pag.proyecto_integrador;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.json.JSONArray;
import org.json.JSONObject;

import Conn.ConexionSql;

public class App {

	public static void circuits() throws CsvValidationException {
	    ConexionSql cc = new ConexionSql();
	    Connection con = cc.getConexion();

	    if (con == null) {
	        System.out.println("La conexión a la base de datos no se pudo establecer.");
	        return;
	    }

	    String path = "C:\\Users\\Ricardo\\Downloads\\proyecto_integrador\\src\\main\\java\\com\\pag\\proyecto_integrador\\CSV's\\circuits.csv";

	    try (
	            CSVReader reader = new CSVReader(new FileReader(path));
	            PreparedStatement pst = con.prepareStatement(
	                "INSERT INTO circuits (circuitId, circuitRef, name, location, country, lat, lng, alt, url) " +
	                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
	                "ON DUPLICATE KEY UPDATE circuitRef=VALUES(circuitRef), name=VALUES(name), location=VALUES(location), country=VALUES(country), lat=VALUES(lat), lng=VALUES(lng), alt=VALUES(alt), url=VALUES(url)")
	        ) {
	            String[] nextLine;
	            reader.readNext(); // Saltar la cabecera

	            List<JSONObject> jsonObjects = new ArrayList<>();

	            while ((nextLine = reader.readNext()) != null) {
	                try {
	                    int circuitId = parseInteger(nextLine[0]);
	                    String circuitRef = parseString(nextLine[1]);
	                    String name = parseString(nextLine[2]);
	                    String location = parseString(nextLine[3]);
	                    String country = parseString(nextLine[4]);
	                    Float lat = parseFloat(nextLine[5]);
	                    Float lng = parseFloat(nextLine[6]);
	                    Float alt = parseFloat(nextLine[7]);
	                    String url = parseString(nextLine[8]);

	                    pst.setObject(1, circuitId);
	                    pst.setObject(2, circuitRef);
	                    pst.setObject(3, name);
	                    pst.setObject(4, location);
	                    pst.setObject(5, country);
	                    pst.setObject(6, lat);
	                    pst.setObject(7, lng);
	                    pst.setObject(8, alt);
	                    pst.setObject(9, url);

	                    pst.addBatch();

	                    // Crear objeto JSON
	                    JSONObject circuitJson = new JSONObject();
	                    circuitJson.put("circuitId", circuitId);
	                    circuitJson.put("circuitRef", circuitRef);
	                    circuitJson.put("name", name);
	                    circuitJson.put("location", location);
	                    circuitJson.put("country", country);
	                    circuitJson.put("lat", lat != null ? lat : JSONObject.NULL);
	                    circuitJson.put("lng", lng != null ? lng : JSONObject.NULL);
	                    circuitJson.put("alt", alt != null ? alt : JSONObject.NULL);
	                    circuitJson.put("url", url);

	                    jsonObjects.add(circuitJson);
	                } catch (NumberFormatException e) {
	                    System.out.println("Error al parsear datos en línea: " + String.join(",", nextLine));
	                }
	            }

	            pst.executeBatch(); // Ejecutar todas las instrucciones en el lote

	            // Escribir datos JSON a un archivo
	            JSONArray jsonArray = new JSONArray(jsonObjects);
	            try (FileWriter file = new FileWriter("C:\\Users\\Ricardo\\Downloads\\proyecto_integrador\\src\\main\\java\\com\\pag\\proyecto_integrador\\Json\\circuits.json")) {
	                file.write(jsonArray.toString(4)); // Indentación de 4 espacios
	            }

	            System.out.println("Datos insertados y registrados en JSON correctamente.");
	        } catch (IOException | SQLException e) {
	            e.printStackTrace();
	            try {
	                if (con != null) {
	                    con.rollback();
	                }
	            } catch (SQLException ex) {
	                ex.printStackTrace();
	            }
	        } finally {
	            try {
	                if (con != null) con.close();
	            } catch (SQLException ex) {
	                ex.printStackTrace();
	            }
	        }
	}
    
    public static void seasons() throws CsvValidationException {
        ConexionSql cc = new ConexionSql();
        Connection con = cc.getConexion();

        if (con == null) {
            System.out.println("La conexión a la base de datos no se pudo establecer.");
            return;
        }

        String path = "C:\\Users\\Ricardo\\Downloads\\proyecto_integrador\\src\\main\\java\\com\\pag\\proyecto_integrador\\CSV's\\seasons.csv";

        try (
            CSVReader reader = new CSVReader(new FileReader(path));
            PreparedStatement pst = con.prepareStatement(
                "INSERT INTO seasons (year, url) VALUES (?, ?) ON DUPLICATE KEY UPDATE url=VALUES(url)")
        ) {
            String[] nextLine;
            reader.readNext(); // Saltar la cabecera

            List<JSONObject> jsonObjects = new ArrayList<>();

            while ((nextLine = reader.readNext()) != null) {
                try {
                    int year = parseInteger(nextLine[0]);
                    String url = nextLine[1];

                    pst.setInt(1, year);
                    pst.setString(2, url);

                    pst.addBatch();

                    // Crear objeto JSON
                    JSONObject seasonJson = new JSONObject();
                    seasonJson.put("year", year);
                    seasonJson.put("url", url);

                    jsonObjects.add(seasonJson);
                } catch (IllegalArgumentException e) {
                    System.out.println("Error al parsear datos en línea: " + String.join(",", nextLine));
                }
            }

            pst.executeBatch(); // Ejecutar todas las instrucciones en el lote

            // Escribir datos JSON a un archivo
            JSONArray jsonArray = new JSONArray(jsonObjects);
            try (FileWriter file = new FileWriter("C:\\Users\\Ricardo\\Downloads\\proyecto_integrador\\src\\main\\java\\com\\pag\\proyecto_integrador\\Json\\seasons.json")) {
                file.write(jsonArray.toString(4)); // Indentación de 4 espacios
            }

            System.out.println("Datos insertados y registrados en JSON correctamente.");
        } catch (IOException | SQLException e) {
            e.printStackTrace();
            try {
                if (con != null) {
                    con.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            try {
                if (con != null) con.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

   public static void races() throws CsvValidationException {
        ConexionSql cc = new ConexionSql();
        Connection con = cc.getConexion();

        if (con == null) {
            System.out.println("La conexión a la base de datos no se pudo establecer.");
            return;
        }

        String path = "C:\\Users\\Ricardo\\Downloads\\proyecto_integrador\\src\\main\\java\\com\\pag\\proyecto_integrador\\CSV's\\races.csv";

        try (
                CSVReader reader = new CSVReader(new FileReader(path));
                PreparedStatement pst = con.prepareStatement(
                    "INSERT INTO races (raceId, year, round, circuitId, name, date, time, url, fp1_date, fp1_time, fp2_date, fp2_time, fp3_date, fp3_time, quali_date, quali_time, sprint_date, sprint_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                    "ON DUPLICATE KEY UPDATE year=VALUES(year), round=VALUES(round), circuitId=VALUES(circuitId), name=VALUES(name), date=VALUES(date), time=VALUES(time), url=VALUES(url), fp1_date=VALUES(fp1_date), fp1_time=VALUES(fp1_time), fp2_date=VALUES(fp2_date), fp2_time=VALUES(fp2_time), fp3_date=VALUES(fp3_date), fp3_time=VALUES(fp3_time), quali_date=VALUES(quali_date), quali_time=VALUES(quali_time), sprint_date=VALUES(sprint_date), sprint_time=VALUES(sprint_time)")
            ) {
                String[] nextLine;
                reader.readNext(); // Saltar la cabecera

                List<JSONObject> jsonObjects = new ArrayList<>();

                while ((nextLine = reader.readNext()) != null) {
                    try {
                        int raceId = parseInteger(nextLine[0]);
                        int year = parseInteger(nextLine[1]);
                        int round = parseInteger(nextLine[2]);
                        int circuitId = parseInteger(nextLine[3]);
                        String name = parseString(nextLine[4]);
                        Date date = parseDate(nextLine[5]);
                        Time time = parseTime(nextLine[6]);
                        String url = parseString(nextLine[7]);
                        Date fp1_date = parseDate(nextLine[8]);
                        Time fp1_time = parseTime(nextLine[9]);
                        Date fp2_date = parseDate(nextLine[10]);
                        Time fp2_time = parseTime(nextLine[11]);
                        Date fp3_date = parseDate(nextLine[12]);
                        Time fp3_time = parseTime(nextLine[13]);
                        Date quali_date = parseDate(nextLine[14]);
                        Time quali_time = parseTime(nextLine[15]);
                        Date sprint_date = parseDate(nextLine[16]);
                        Time sprint_time = parseTime(nextLine[17]);

                        pst.setObject(1, raceId);
                        pst.setObject(2, year);
                        pst.setObject(3, round);
                        pst.setObject(4, circuitId);
                        pst.setObject(5, name);
                        pst.setObject(6, date);
                        pst.setObject(7, time);
                        pst.setObject(8, url);
                        pst.setObject(9, fp1_date);
                        pst.setObject(10, fp1_time);
                        pst.setObject(11, fp2_date);
                        pst.setObject(12, fp2_time);
                        pst.setObject(13, fp3_date);
                        pst.setObject(14, fp3_time);
                        pst.setObject(15, quali_date);
                        pst.setObject(16, quali_time);
                        pst.setObject(17, sprint_date);
                        pst.setObject(18, sprint_time);

                        pst.addBatch();

                        // Crear objeto JSON
                        JSONObject raceJson = new JSONObject();
                        raceJson.put("raceId", raceId);
                        raceJson.put("year", year);
                        raceJson.put("round", round);
                        raceJson.put("circuitId", circuitId);
                        raceJson.put("name", name);
                        raceJson.put("date", date != null ? date.toString() : null);
                        raceJson.put("time", time != null ? time.toString() : null);
                        raceJson.put("url", url);
                        raceJson.put("fp1_date", fp1_date != null ? fp1_date.toString() : null);
                        raceJson.put("fp1_time", fp1_time != null ? fp1_time.toString() : null);
                        raceJson.put("fp2_date", fp2_date != null ? fp2_date.toString() : null);
                        raceJson.put("fp2_time", fp2_time != null ? fp2_time.toString() : null);
                        raceJson.put("fp3_date", fp3_date != null ? fp3_date.toString() : null);
                        raceJson.put("fp3_time", fp3_time != null ? fp3_time.toString() : null);
                        raceJson.put("quali_date", quali_date != null ? quali_date.toString() : null);
                        raceJson.put("quali_time", quali_time != null ? quali_time.toString() : null);
                        raceJson.put("sprint_date", sprint_date != null ? sprint_date.toString() : null);
                        raceJson.put("sprint_time", sprint_time != null ? sprint_time.toString() : null);

                        jsonObjects.add(raceJson);
                    } catch (IllegalArgumentException e) {
                        System.out.println("Error al parsear datos en línea: " + String.join(",", nextLine));
                    }
                }

                pst.executeBatch(); // Ejecutar todas las instrucciones en el lote

                // Escribir datos JSON a un archivo
                JSONArray jsonArray = new JSONArray(jsonObjects);
                try (FileWriter file = new FileWriter("C:\\Users\\Ricardo\\Downloads\\proyecto_integrador\\src\\main\\java\\com\\pag\\proyecto_integrador\\Json\\races.json")) {
                    file.write(jsonArray.toString(4)); // Indentación de 4 espacios
                }

                System.out.println("Datos insertados y registrados en JSON correctamente.");
            } catch (IOException | SQLException e) {
                e.printStackTrace();
                try {
                    if (con != null) {
                        con.rollback();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            } finally {
                try {
                    if (con != null) con.close();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
    }

    private static Integer parseInteger(String value) {
        return "N".equals(value) ? null : Integer.parseInt(value);
    }

    private static String parseString(String value) {
        return "N".equals(value) ? null : value;
    }

    private static Date parseDate(String value) {
        return "N".equals(value) ? null : Date.valueOf(value);
    }

    private static Time parseTime(String value) {
        return "N".equals(value) ? null : Time.valueOf(value);
    }
    
    private static Float parseFloat(String value) {
        return "N".equals(value) ? null : Float.parseFloat(value);
    }
    public static void results() throws CsvValidationException {
        ConexionSql cc = new ConexionSql();
        Connection con = cc.getConexion();

        if (con == null) {
            System.out.println("La conexión a la base de datos no se pudo establecer.");
            return;
        }

        String path = "C:\\Users\\Ricardo\\Downloads\\proyecto_integrador\\src\\main\\java\\com\\pag\\proyecto_integrador\\CSV's\\results.csv";

        try (
            CSVReader reader = new CSVReader(new FileReader(path));
            PreparedStatement pst = con.prepareStatement(
                "INSERT INTO results (resultId, raceId, driverId, constructorId, number, grid, position, positionText, positionOrder, points, laps, time, miliseconds, fastestLap, rank, fastestLapTime, fastestLapSpeed, statusId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE raceId=VALUES(raceId), driverId=VALUES(driverId), constructorId=VALUES(constructorId), number=VALUES(number), grid=VALUES(grid), position=VALUES(position), positionText=VALUES(positionText), positionOrder=VALUES(positionOrder), points=VALUES(points), laps=VALUES(laps), time=VALUES(time), miliseconds=VALUES(miliseconds), fastestLap=VALUES(fastestLap), rank=VALUES(rank), fastestLapTime=VALUES(fastestLapTime), fastestLapSpeed=VALUES(fastestLapSpeed), statusId=VALUES(statusId)")
        ) {
            String[] nextLine;
            reader.readNext(); // Saltar la cabecera

            List<JSONObject> jsonObjects = new ArrayList<>();

            while ((nextLine = reader.readNext()) != null) {
                try {
                    int resultId = Integer.parseInt(nextLine[0]);
                    int raceId = Integer.parseInt(nextLine[1]);
                    int driverId = Integer.parseInt(nextLine[2]);
                    int constructorId = Integer.parseInt(nextLine[3]);
                    int number = Integer.parseInt(nextLine[4]);
                    int grid = Integer.parseInt(nextLine[5]);
                    int position = Integer.parseInt(nextLine[6]);
                    String positionText = nextLine[7];
                    int positionOrder = Integer.parseInt(nextLine[8]);
                    float points = Float.parseFloat(nextLine[9]);
                    int laps = Integer.parseInt(nextLine[10]);
                    String time = nextLine[11];
                    int miliseconds = Integer.parseInt(nextLine[12]);
                    int fastestLap = Integer.parseInt(nextLine[13]);
                    int rank = Integer.parseInt(nextLine[14]);
                    String fastestLapTime = nextLine[15];
                    String fastestLapSpeed = nextLine[16];
                    int statusId = Integer.parseInt(nextLine[17]);

                    pst.setInt(1, resultId);
                    pst.setInt(2, raceId);
                    pst.setInt(3, driverId);
                    pst.setInt(4, constructorId);
                    pst.setInt(5, number);
                    pst.setInt(6, grid);
                    pst.setInt(7, position);
                    pst.setString(8, positionText);
                    pst.setInt(9, positionOrder);
                    pst.setFloat(10, points);
                    pst.setInt(11, laps);
                    pst.setString(12, time);
                    pst.setInt(13, miliseconds);
                    pst.setInt(14, fastestLap);
                    pst.setInt(15, rank);
                    pst.setString(16, fastestLapTime);
                    pst.setString(17, fastestLapSpeed);
                    pst.setInt(18, statusId);

                    pst.addBatch();

                    // Crear objeto JSON
                    JSONObject resultJson = new JSONObject();
                    resultJson.put("resultId", resultId);
                    resultJson.put("raceId", raceId);
                    resultJson.put("driverId", driverId);
                    resultJson.put("constructorId", constructorId);
                    resultJson.put("number", number);
                    resultJson.put("grid", grid);
                    resultJson.put("position", position);
                    resultJson.put("positionText", positionText);
                    resultJson.put("positionOrder", positionOrder);
                    resultJson.put("points", points);
                    resultJson.put("laps", laps);
                    resultJson.put("time", time);
                    resultJson.put("miliseconds", miliseconds);
                    resultJson.put("fastestLap", fastestLap);
                    resultJson.put("rank", rank);
                    resultJson.put("fastestLapTime", fastestLapTime);
                    resultJson.put("fastestLapSpeed", fastestLapSpeed);
                    resultJson.put("statusId", statusId);

                    jsonObjects.add(resultJson);
                } catch (IllegalArgumentException e) {
                    System.out.println("Error al parsear datos en línea: " + String.join(",", nextLine));
                }
            }

            pst.executeBatch(); // Ejecutar todas las instrucciones en el lote

            // Escribir datos JSON a un archivo
            JSONArray jsonArray = new JSONArray(jsonObjects);
            try (FileWriter file = new FileWriter("C:\\Users\\Ricardo\\Downloads\\proyecto_integrador\\src\\main\\java\\com\\pag\\proyecto_integrador\\Json\\results.json")) {
                file.write(jsonArray.toString(4)); // Indentación de 4 espacios
            }

            System.out.println("Datos insertados y registrados en JSON correctamente.");
        } catch (IOException | SQLException e) {
            e.printStackTrace();
            try {
                if (con != null) {
                    con.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            try {
                if (con != null) con.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void drivers() throws CsvValidationException {
        ConexionSql cc = new ConexionSql();
        Connection con = cc.getConexion();

        if (con == null) {
            System.out.println("La conexión a la base de datos no se pudo establecer.");
            return;
        }

        String path = "C:\\Users\\Ricardo\\Downloads\\proyecto_integrador\\src\\main\\java\\com\\pag\\proyecto_integrador\\CSV's\\drivers.csv";

        try (
            CSVReader reader = new CSVReader(new FileReader(path));
            PreparedStatement pst = con.prepareStatement(
                "INSERT INTO drivers (driverId, driverRef, number, code, forename, surname, dob, nationality, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE driverRef=VALUES(driverRef), number=VALUES(number), code=VALUES(code), forename=VALUES(forename), surname=VALUES(surname), dob=VALUES(dob), nationality=VALUES(nationality), url=VALUES(url)")
        ) {
            String[] nextLine;
            reader.readNext(); // Saltar la cabecera

            List<JSONObject> jsonObjects = new ArrayList<>();

            while ((nextLine = reader.readNext()) != null) {
                try {
                    Integer driverId = parseInteger(nextLine[0]);
                    String driverRef = nextLine[1];
                    Integer number = parseInteger(nextLine[2]);
                    String code = nextLine[3];
                    String forename = nextLine[4];
                    String surname = nextLine[5];
                    Date dob = parseDate(nextLine[6]);
                    String nationality = nextLine[7];
                    String url = nextLine[8];

                    if (driverId != null) {
                        pst.setInt(1, driverId);
                    } else {
                        pst.setNull(1, java.sql.Types.INTEGER);
                    }

                    pst.setString(2, driverRef);

                    if (number != null) {
                        pst.setInt(3, number);
                    } else {
                        pst.setNull(3, java.sql.Types.INTEGER);
                    }

                    pst.setString(4, code);
                    pst.setString(5, forename);
                    pst.setString(6, surname);
                    pst.setDate(7, dob);
                    pst.setString(8, nationality);
                    pst.setString(9, url);

                    pst.addBatch();

                    // Crear objeto JSON
                    JSONObject driverJson = new JSONObject();
                    driverJson.put("driverId", driverId);
                    driverJson.put("driverRef", driverRef);
                    driverJson.put("number", number);
                    driverJson.put("code", code);
                    driverJson.put("forename", forename);
                    driverJson.put("surname", surname);
                    driverJson.put("dob", dob.toString());
                    driverJson.put("nationality", nationality);
                    driverJson.put("url", url);

                    jsonObjects.add(driverJson);
                } catch (IllegalArgumentException e) {
                    System.out.println("Error al parsear datos en línea: " + String.join(",", nextLine));
                }
            }

            pst.executeBatch(); // Ejecutar todas las instrucciones en el lote

            // Escribir datos JSON a un archivo
            JSONArray jsonArray = new JSONArray(jsonObjects);
            try (FileWriter file = new FileWriter("C:\\Users\\Ricardo\\Downloads\\proyecto_integrador\\src\\main\\java\\com\\pag\\proyecto_integrador\\Json\\drivers.json")) {
                file.write(jsonArray.toString(4)); // Indentación de 4 espacios
            }

            System.out.println("Datos insertados y registrados en JSON correctamente.");
        } catch (IOException | SQLException e) {
            e.printStackTrace();
            try {
                if (con != null) {
                    con.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            try {
                if (con != null) con.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }


    public static void constructors() throws CsvValidationException {
        ConexionSql cc = new ConexionSql();
        Connection con = cc.getConexion();

        if (con == null) {
            System.out.println("La conexión a la base de datos no se pudo establecer.");
            return;
        }

        String path = "C:\\Users\\Ricardo\\Downloads\\proyecto_integrador\\src\\main\\java\\com\\pag\\proyecto_integrador\\CSV's\\constructors.csv";

        try (
            CSVReader reader = new CSVReader(new FileReader(path));
            PreparedStatement pst = con.prepareStatement(
                "INSERT INTO constructors (constructorId, constructorRef, name, nationality, url) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE constructorRef=VALUES(constructorRef), name=VALUES(name), nationality=VALUES(nationality), url=VALUES(url)")
        ) {
            String[] nextLine;
            reader.readNext(); // Saltar la cabecera

            List<JSONObject> jsonObjects = new ArrayList<>();

            while ((nextLine = reader.readNext()) != null) {
                try {
                    int constructorId = parseInteger(nextLine[0]);
                    String constructorRef = nextLine[1];
                    String name = nextLine[2];
                    String nationality = nextLine[3];
                    String url = nextLine[4];

                    pst.setInt(1, constructorId);
                    pst.setString(2, constructorRef);
                    pst.setString(3, name);
                    pst.setString(4, nationality);
                    pst.setString(5, url);

                    pst.addBatch();

                    // Crear objeto JSON
                    JSONObject constructorJson = new JSONObject();
                    constructorJson.put("constructorId", constructorId);
                    constructorJson.put("constructorRef", constructorRef);
                    constructorJson.put("name", name);
                    constructorJson.put("nationality", nationality);
                    constructorJson.put("url", url);

                    jsonObjects.add(constructorJson);
                } catch (IllegalArgumentException e) {
                    System.out.println("Error al parsear datos en línea: " + String.join(",", nextLine));
                }
            }

            pst.executeBatch(); // Ejecutar todas las instrucciones en el lote

            // Escribir datos JSON a un archivo
            JSONArray jsonArray = new JSONArray(jsonObjects);
            try (FileWriter file = new FileWriter("C:\\Users\\Ricardo\\Downloads\\proyecto_integrador\\src\\main\\java\\com\\pag\\proyecto_integrador\\Json\\constructors.json")) {
                file.write(jsonArray.toString(4)); // Indentación de 4 espacios
            }

            System.out.println("Datos insertados y registrados en JSON correctamente.");
        } catch (IOException | SQLException e) {
            e.printStackTrace();
            try {
                if (con != null) {
                    con.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            try {
                if (con != null) con.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void lap_times() throws CsvValidationException {
        ConexionSql cc = new ConexionSql();
        Connection con = cc.getConexion();

        if (con == null) {
            System.out.println("La conexión a la base de datos no se pudo establecer.");
            return;
        }

        String path = "C:\\Users\\Ricardo\\Downloads\\proyecto_integrador\\src\\main\\java\\com\\pag\\proyecto_integrador\\CSV's\\lap_times.csv";

        try (
            CSVReader reader = new CSVReader(new FileReader(path));
            PreparedStatement pst = con.prepareStatement(
                "INSERT INTO lap_times (raceId, driverId, lap, position, time, miliseconds) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE position=VALUES(position), time=VALUES(time), miliseconds=VALUES(miliseconds)")
        ) {
            String[] nextLine;
            reader.readNext(); // Saltar la cabecera

            List<JSONObject> jsonObjects = new ArrayList<>();

            while ((nextLine = reader.readNext()) != null) {
                try {
                    int raceId = Integer.parseInt(nextLine[0]);
                    int driverId = Integer.parseInt(nextLine[1]);
                    int lap = Integer.parseInt(nextLine[2]);
                    int position = Integer.parseInt(nextLine[3]);
                    String time = nextLine[4];
                    int miliseconds = Integer.parseInt(nextLine[5]);

                    pst.setInt(1, raceId);
                    pst.setInt(2, driverId);
                    pst.setInt(3, lap);
                    pst.setInt(4, position);
                    pst.setString(5, time);
                    pst.setInt(6, miliseconds);

                    pst.addBatch();

                    // Crear objeto JSON
                    JSONObject lapTimeJson = new JSONObject();
                    lapTimeJson.put("raceId", raceId);
                    lapTimeJson.put("driverId", driverId);
                    lapTimeJson.put("lap", lap);
                    lapTimeJson.put("position", position);
                    lapTimeJson.put("time", time);
                    lapTimeJson.put("miliseconds", miliseconds);

                    jsonObjects.add(lapTimeJson);
                } catch (IllegalArgumentException e) {
                    System.out.println("Error al parsear datos en línea: " + String.join(",", nextLine));
                }
            }

            pst.executeBatch(); // Ejecutar todas las instrucciones en el lote

            // Escribir datos JSON a un archivo
            JSONArray jsonArray = new JSONArray(jsonObjects);
            try (FileWriter file = new FileWriter("C:\\Users\\Ricardo\\Downloads\\proyecto_integrador\\src\\main\\java\\com\\pag\\proyecto_integrador\\Json\\lap_times.json")) {
                file.write(jsonArray.toString(4)); // Indentación de 4 espacios
            }

            System.out.println("Datos insertados y registrados en JSON correctamente.");
        } catch (IOException | SQLException e) {
            e.printStackTrace();
            try {
                if (con != null) {
                    con.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            try {
                if (con != null) con.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }


    public static void pit_stops() throws CsvValidationException {
        ConexionSql cc = new ConexionSql();
        Connection con = cc.getConexion();

        if (con == null) {
            System.out.println("La conexión a la base de datos no se pudo establecer.");
            return;
        }

        String path = "C:\\Users\\Ricardo\\Downloads\\proyecto_integrador\\src\\main\\java\\com\\pag\\proyecto_integrador\\CSV's\\pit_stops.csv";

        try (
            CSVReader reader = new CSVReader(new FileReader(path));
            PreparedStatement pst = con.prepareStatement(
                "INSERT INTO pit_stops (raceId, driverId, stop, lap, time, duration, miliseconds) VALUES (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE lap=VALUES(lap), time=VALUES(time), duration=VALUES(duration), miliseconds=VALUES(miliseconds)")
        ) {
            String[] nextLine;
            reader.readNext(); // Saltar la cabecera

            List<JSONObject> jsonObjects = new ArrayList<>();

            while ((nextLine = reader.readNext()) != null) {
                try {
                    int raceId = Integer.parseInt(nextLine[0]);
                    int driverId = Integer.parseInt(nextLine[1]);
                    int stop = Integer.parseInt(nextLine[2]);
                    int lap = Integer.parseInt(nextLine[3]);
                    String time = nextLine[4];
                    String duration = nextLine[5];
                    int miliseconds = Integer.parseInt(nextLine[6]);

                    pst.setInt(1, raceId);
                    pst.setInt(2, driverId);
                    pst.setInt(3, stop);
                    pst.setInt(4, lap);
                    pst.setString(5, time);
                    pst.setString(6, duration);
                    pst.setInt(7, miliseconds);

                    pst.addBatch();

                    // Crear objeto JSON
                    JSONObject pitStopJson = new JSONObject();
                    pitStopJson.put("raceId", raceId);
                    pitStopJson.put("driverId", driverId);
                    pitStopJson.put("stop", stop);
                    pitStopJson.put("lap", lap);
                    pitStopJson.put("time", time);
                    pitStopJson.put("duration", duration);
                    pitStopJson.put("miliseconds", miliseconds);

                    jsonObjects.add(pitStopJson);
                } catch (IllegalArgumentException e) {
                    System.out.println("Error al parsear datos en línea: " + String.join(",", nextLine));
                }
            }

            pst.executeBatch(); // Ejecutar todas las instrucciones en el lote

            // Escribir datos JSON a un archivo
            JSONArray jsonArray = new JSONArray(jsonObjects);
            try (FileWriter file = new FileWriter("C:\\Users\\Ricardo\\Downloads\\proyecto_integrador\\src\\main\\java\\com\\pag\\proyecto_integrador\\Json\\pit_stops.json")) {
                file.write(jsonArray.toString(4)); // Indentación de 4 espacios
            }

            System.out.println("Datos insertados y registrados en JSON correctamente.");
        } catch (IOException | SQLException e) {
            e.printStackTrace();
            try {
                if (con != null) {
                    con.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            try {
                if (con != null) con.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void qualifying() throws CsvValidationException {
        ConexionSql cc = new ConexionSql();
        Connection con = cc.getConexion();

        if (con == null) {
            System.out.println("La conexión a la base de datos no se pudo establecer.");
            return;
        }

        String path = "C:\\Users\\Ricardo\\Downloads\\proyecto_integrador\\src\\main\\java\\com\\pag\\proyecto_integrador\\CSV's\\qualifying.csv";

        try (
            CSVReader reader = new CSVReader(new FileReader(path));
            PreparedStatement pst = con.prepareStatement(
                "INSERT INTO qualifying (qualifyId, raceId, driverId, constructorId, number, position, q1, q2, q3) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE raceId=VALUES(raceId), driverId=VALUES(driverId), constructorId=VALUES(constructorId), number=VALUES(number), position=VALUES(position), q1=VALUES(q1), q2=VALUES(q2), q3=VALUES(q3)")
        ) {
            String[] nextLine;
            reader.readNext(); // Saltar la cabecera

            List<JSONObject> jsonObjects = new ArrayList<>();

            while ((nextLine = reader.readNext()) != null) {
                try {
                    int qualifyId = parseInteger(nextLine[0]);
                    int raceId = parseInteger(nextLine[1]);
                    int driverId = parseInteger(nextLine[2]);
                    int constructorId = parseInteger(nextLine[3]);
                    int number = parseInteger(nextLine[4]);
                    int position = parseInteger(nextLine[5]);
                    String q1 = nextLine[6];
                    String q2 = nextLine[7];
                    String q3 = nextLine[8];

                    pst.setInt(1, qualifyId);
                    pst.setInt(2, raceId);
                    pst.setInt(3, driverId);
                    pst.setInt(4, constructorId);
                    pst.setInt(5, number);
                    pst.setInt(6, position);
                    pst.setString(7, q1);
                    pst.setString(8, q2);
                    pst.setString(9, q3);

                    pst.addBatch();

                    // Crear objeto JSON
                    JSONObject qualifyingJson = new JSONObject();
                    qualifyingJson.put("qualifyId", qualifyId);
                    qualifyingJson.put("raceId", raceId);
                    qualifyingJson.put("driverId", driverId);
                    qualifyingJson.put("constructorId", constructorId);
                    qualifyingJson.put("number", number);
                    qualifyingJson.put("position", position);
                    qualifyingJson.put("q1", q1);
                    qualifyingJson.put("q2", q2);
                    qualifyingJson.put("q3", q3);

                    jsonObjects.add(qualifyingJson);
                } catch (IllegalArgumentException e) {
                    System.out.println("Error al parsear datos en línea: " + String.join(",", nextLine));
                }
            }

            pst.executeBatch(); // Ejecutar todas las instrucciones en el lote

            // Escribir datos JSON a un archivo
            JSONArray jsonArray = new JSONArray(jsonObjects);
            try (FileWriter file = new FileWriter("C:\\Users\\Ricardo\\Downloads\\proyecto_integrador\\src\\main\\java\\com\\pag\\proyecto_integrador\\Json\\qualifying.json")) {
                file.write(jsonArray.toString(4)); // Indentación de 4 espacios
            }

            System.out.println("Datos insertados y registrados en JSON correctamente.");
        } catch (IOException | SQLException e) {
            e.printStackTrace();
            try {
                if (con != null) {
                    con.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            try {
                if (con != null) con.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }


    public static void main(String[] args) throws CsvValidationException {
        circuits();
        seasons();
        races();
        results();
        drivers();
        constructors();
        lap_times();
        pit_stops();
        qualifying();
    }
}
