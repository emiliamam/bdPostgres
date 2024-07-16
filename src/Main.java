package bd;

import java.sql.*;

public class Main {
    private final static String PROTOCOL = "jdbc:postgresql://";
    private final static String DRIVER = "org.postgresql.Driver";
    private final static String URL_LOCALE_NAME = "localhost/";
    private final static String DATABASE_NAME = "concert";
    private final static String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    private final static String USER_NAME = "postgres";
    private final static String DATABASE_PASSWORD = "postgres";

    public static void main(String[] args) {
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASSWORD)) {
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 1. Вывод всех концертов");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            getAllConcerts(connection);

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 2. Добавление нового концерта");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            addConcert(connection, "The best concert in France", new Date(System.currentTimeMillis()), "France", 4, 1);
            getAllConcerts(connection);

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 3. Обновление значения");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            updateConcertName(connection, 1, "OMG ");
            getAllConcerts(connection);

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 4. Вывод всех билетов на концерт c id = 2");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            getAllTicketsForConcert(connection, 2);

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 5. Вывод всех концертов c 13 по 17 июля ");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            getAllConcertsInPeriod(connection, java.sql.Date.valueOf("2024-07-13"), java.sql.Date.valueOf("2024-07-17"));

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 6. Вывод всех концертов исполнителя Lana");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            getAllConcertsByPerformerName(connection, "Lana");

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 7. Вывод всех концертов, проведенных в CSKA");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            getAllConcertsByVenue(connection, "CSKA");

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 8. Подсчёт количества концертов, проведённых в каждом месте");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            countConcertsByLocation(connection);

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 9. Удаление записи по id");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            deleteConcertWithTickets(connection, 1);
            getAllConcerts(connection);

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 10. Поиск всех концертов с билетами дороже 100");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            getAllTicketsAbovePrice(connection, 100);

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 11. Вывод всех концертов, которые проходят после 1 июня");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            getAllConcertsAfterDate(connection, Date.valueOf("2024-06-01"));

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 14. Вывод всех концертов и их исполнителей");
            getConcertsAndPerformers(connection);
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");

        } catch (SQLException e) {
            System.out.println("Exception");
        }
    }

    public static void checkDriver() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBS-драйвера!");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB() {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASSWORD);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    //    1 запрос. Вывод всех концертов
    public static void getAllConcerts(Connection connection) throws SQLException {
        String query = "SELECT * FROM concert";

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                Date date = resultSet.getDate("date");
                String venue = resultSet.getString("venue");
                int performerId = resultSet.getInt("performer_id");
                int organizerId = resultSet.getInt("organizer_id");

                System.out.printf("ID: %d, Name: %s, Date: %s, Venue: %s, Performer ID: %d, Organizer ID: %d\n",
                        id, name, date.toString(), venue, performerId, organizerId);
            }
        }
    }

    //    2 запрос. Добавление концерта

    public static void addConcert(Connection connection, String name, Date date, String venue, int performerId, int organizerId) throws SQLException {
        String query = "INSERT INTO concert (name, date, venue, performer_id, organizer_id) VALUES (?, ?, ?, ?, ?)";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, name);
            statement.setDate(2, new java.sql.Date(date.getTime()));
            statement.setString(3, venue);
            statement.setInt(4, performerId);
            statement.setInt(5, organizerId);

            statement.executeUpdate();
            System.out.println("Concert added successfully");
        } catch (Error e){
            System.out.println(e.getMessage());
        }
    }

    //    3 запрос. Обновление значения


    public static void updateConcertName(Connection connection, int concertId, String newName) throws SQLException {
        String query = "UPDATE concert SET name = ? WHERE id = ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, newName);
            statement.setInt(2, concertId);

            statement.executeUpdate();
            System.out.println("Concert updated successfully");
        }
    }




    //    4 запрос. Вывод всех билетов на конкретный концерт

    public static void getAllTicketsForConcert(Connection connection, int concertId) throws SQLException {
        String query = "SELECT * FROM ticket WHERE concert_id = ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, concertId);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    String ticketNumber = resultSet.getString("ticket_number");
                    int concertIdFromDb = resultSet.getInt("concert_id");
                    Date purchaseDate = resultSet.getDate("purchase_date");
                    double price = resultSet.getDouble("price");

                    System.out.printf("ID: %d, Ticket Number: %s, Concert ID: %d, Purchase Date: %s, Price: %.2f\n",
                            id, ticketNumber, concertIdFromDb, purchaseDate.toString(), price);
                }
            }
        }
    }

    //    5 запрос. Вывод всех концертов c 13 по 17 июля
    public static void getAllConcertsInPeriod(Connection connection, Date startDate, Date endDate) throws SQLException {
        String query = "SELECT * FROM concert WHERE date BETWEEN ? AND ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setDate(1, new java.sql.Date(startDate.getTime()));
            statement.setDate(2, new java.sql.Date(endDate.getTime()));

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    String name = resultSet.getString("name");
                    Date date = resultSet.getDate("date");
                    String venue = resultSet.getString("venue");
                    int performerId = resultSet.getInt("performer_id");
                    int organizerId = resultSet.getInt("organizer_id");

                    System.out.printf("ID: %d, Name: %s, Date: %s, Venue: %s, Performer ID: %d, Organizer ID: %d\n",
                            id, name, date.toString(), venue, performerId, organizerId);
                }
            }
        }
    }

    //    6 запрос. Вывод всех концертов исполнителя Lana

    public static void getAllConcertsByPerformerName(Connection connection, String performerName) throws SQLException {
        String query = "SELECT concert.* FROM concert " +
                "JOIN performer ON concert.performer_id = performer.id " +
                "WHERE performer.name = ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, performerName);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    String name = resultSet.getString("name");
                    Date date = resultSet.getDate("date");
                    String venue = resultSet.getString("venue");
                    int performerId = resultSet.getInt("performer_id");
                    int organizerId = resultSet.getInt("organizer_id");

                    System.out.printf("ID: %d, Name: %s, Date: %s, Venue: %s, Performer ID: %d, Organizer ID: %d\n",
                            id, name, date.toString(), venue, performerId, organizerId);
                }
            }
        }
    }


    //    7 запрос. Вывод всех концертов, проведенных в CSKA

    public static void getAllConcertsByVenue(Connection connection, String venue) throws SQLException {
        String query = "SELECT * FROM concert WHERE venue = ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, venue);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    String name = resultSet.getString("name");
                    Date date = resultSet.getDate("date");
                    String venueFromDb = resultSet.getString("venue");
                    int performerId = resultSet.getInt("performer_id");
                    int organizerId = resultSet.getInt("organizer_id");

                    System.out.printf("ID: %d, Name: %s, Date: %s, Venue: %s, Performer ID: %d, Organizer ID: %d\n",
                            id, name, date.toString(), venueFromDb, performerId, organizerId);
                }
            }
        }
    }
    //    8 запрос


    public static void countConcertsByLocation(Connection connection) throws SQLException {
        String query = "SELECT venue, COUNT(*) AS concert_count FROM concert GROUP BY venue";

        try (PreparedStatement statement = connection.prepareStatement(query);
             ResultSet resultSet = statement.executeQuery()) {

            while (resultSet.next()) {
                String venue = resultSet.getString("venue");
                int concertCount = resultSet.getInt("concert_count");
                System.out.println("Venue: " + venue + ", Count: " + concertCount);
            }
        }
    }

    //    9 запрос. Удаление записи по id
    public static void deleteConcertWithTickets(Connection connection, int concertId) throws SQLException {
        String deleteTicketsQuery = "DELETE FROM ticket WHERE concert_id = ?";
        String deleteConcertQuery = "DELETE FROM concert WHERE id = ?";

        try (PreparedStatement deleteTicketsStmt = connection.prepareStatement(deleteTicketsQuery);
             PreparedStatement deleteConcertStmt = connection.prepareStatement(deleteConcertQuery)) {

            deleteTicketsStmt.setInt(1, concertId);
            deleteTicketsStmt.executeUpdate();

            deleteConcertStmt.setInt(1, concertId);
            int rowsAffected = deleteConcertStmt.executeUpdate();

            if (rowsAffected > 0) {
                System.out.println("All delete");
            } else {
                System.out.println("No concert with this ID");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }



    //    10 запрос. Поиск всех концертов с билетами дороже 1000

    public static void getAllTicketsAbovePrice(Connection connection, double price) throws SQLException {
        String query = "SELECT ticket.ticket_number, ticket.concert_id, ticket.purchase_date, ticket.price, " +
                "concert.id, concert.name, concert.date, concert.venue, concert.performer_id, concert.organizer_id " +
                "FROM concert " +
                "JOIN ticket ON concert.id = ticket.concert_id " +
                "WHERE ticket.price > ?";


        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setDouble(1, price);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    String ticketNumber = resultSet.getString("ticket_number");
                    int concertId = resultSet.getInt("concert_id");
                    Date purchaseDate = resultSet.getDate("purchase_date");
                    double priceFromDb = resultSet.getDouble("price");

                    System.out.printf("ID: %d, Ticket Number: %s, Concert ID: %d, Purchase Date: %s, Price: %.2f\n",
                            id, ticketNumber, concertId, purchaseDate.toString(), priceFromDb);
                }
            } catch (Exception e){
                System.out.println(e.getMessage());
            }
        }
    }

    //    11 запрос. Вывод всех концертов, которые проходят после 1 июня

    public static void getAllConcertsAfterDate(Connection connection, Date date) throws SQLException {
        String query = "SELECT * FROM concert WHERE date > ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setDate(1, date);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    String name = resultSet.getString("name");
                    Date dateFromDb = resultSet.getDate("date");
                    String venue = resultSet.getString("venue");
                    int performerId = resultSet.getInt("performer_id");
                    int organizerId = resultSet.getInt("organizer_id");

                    System.out.printf("ID: %d, Name: %s, Date: %s, Venue: %s, Performer ID: %d, Organizer ID: %d\n",
                            id, name, dateFromDb.toString(), venue, performerId, organizerId);
                }
            }
        }
    }


    //    12 запрос. Вывод всех концертов и их исполнителей

    public static void getConcertsAndPerformers(Connection connection) throws SQLException {
        String query = "SELECT c.name AS concert_name, c.date, p.name AS performer_name, p.genre " +
                "FROM concert c " +
                "JOIN performer p ON c.performer_id = p.id";

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            while (resultSet.next()) {
                String concertName = resultSet.getString("concert_name");
                String date = resultSet.getString("date");
                String performerName = resultSet.getString("performer_name");
                String genre = resultSet.getString("genre");

                System.out.println("Concert: " + concertName + ", Date: " + date +
                        ", Performer: " + performerName + ", Genre: " + genre);
            }
        }
    }
}
