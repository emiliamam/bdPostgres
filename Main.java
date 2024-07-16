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
            // Вывод всех концертов
            getAllConcerts(connection);

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 2. Вывод всех исполнителей");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            // Вывод всех исполнителей
            getAllPerformers(connection);

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 3. Вывод всех организаторов");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            // Вывод всех организаторов
            getAllOrganizers(connection);

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 4. Вывод всех билетов на конкретный концерт");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            // Вывод всех билетов на конкретный концерт
            getAllTicketsForConcert(connection, 1);

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 5. Вывод всех концертов, организованных конкретным организатором");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            // Вывод всех концертов, организованных конкретным организатором
            getAllConcertsByOrganizer(connection, 1);

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 6. Вывод всех концертов конкретного исполнителя");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            // Вывод всех концертов конкретного исполнителя
            getAllConcertsByPerformer(connection, 1);

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 7. Вывод всех концертов, проведенных в определенном месте");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            // Вывод всех концертов, проведенных в определенном месте
            getAllConcertsByVenue(connection, "CSKA");

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 8. Вывод всех билетов, купленных в определенную дату");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            // Вывод всех билетов, купленных в определенную дату
            getAllTicketsByDate(connection, Date.valueOf("2024-07-17"));

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 9. Вывод всех исполнителей из определенной страны");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            // Вывод всех исполнителей из определенной страны
            getAllPerformersByCountry(connection, "USA");

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 10. Вывод всех билетов, цена которых выше определенного значения");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            // Вывод всех билетов, цена которых выше определенного значения
            getAllTicketsAbovePrice(connection, 1000);


            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 11. Вывод всех концертов, которые проходят после определенной даты");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            // Вывод всех концертов, которые проходят после определенной даты
            getAllConcertsAfterDate(connection, Date.valueOf("2024-06-01"));

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 12. Вывод всех концертов, у которых цена билета ниже определенного значения");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            // Вывод всех концертов, у которых цена билета ниже определенного значения
            getAllConcertsBelowTicketPrice(connection, 3000);

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 13. Вывод всех организаторов с определенным контактным номером");
            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            // Вывод всех организаторов с определенным контактным номером
            getAllOrganizersByContactNumber(connection, "+7 925 000 00 00");

            System.out.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Запрос 14. Вывод всех концертов и их исполнителей");
            // Вывод всех концертов и их исполнителей.
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

//    1 запрос
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

    //    2 запрос

    public static void getAllPerformers(Connection connection) throws SQLException {
        String query = "SELECT * FROM performer";

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                String genre = resultSet.getString("genre");
                String country = resultSet.getString("country");

                System.out.printf("ID: %d, Name: %s, Genre: %s, Country: %s: \n",
                        id, name, genre, country);
            }
        }
    }

    //    3 запрос


    public static void getAllOrganizers(Connection connection) throws SQLException {
        String query = "SELECT * FROM organizer";

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                String contactPhone = resultSet.getString("contact_phone");
                String email = resultSet.getString("email");

                System.out.printf("ID: %d, Name: %s, Contact Phone: %s, Email: %s: \n",
                        id, name, contactPhone, email);
            }
        }
    }

    //    4 запрос

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

    //    5 запрос


    public static void getAllConcertsByOrganizer(Connection connection, int organizerId) throws SQLException {
        String query = "SELECT * FROM concert WHERE organizer_id = ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, organizerId);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    String name = resultSet.getString("name");
                    Date date = resultSet.getDate("date");
                    String venue = resultSet.getString("venue");
                    int performerId = resultSet.getInt("performer_id");
                    int organizerIdFromDb = resultSet.getInt("organizer_id");

                    System.out.printf("ID: %d, Name: %s, Date: %s, Venue: %s, Performer ID: %d, Organizer ID: %d\n",
                            id, name, date.toString(), venue, performerId, organizerIdFromDb);
                }
            }
        }
    }

    //    6 запрос

    public static void getAllConcertsByPerformer(Connection connection, int performerId) throws SQLException {
        String query = "SELECT * FROM concert WHERE performer_id = ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, performerId);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    String name = resultSet.getString("name");
                    Date date = resultSet.getDate("date");
                    String venue = resultSet.getString("venue");
                    int performerIdFromDb = resultSet.getInt("performer_id");
                    int organizerId = resultSet.getInt("organizer_id");

                    System.out.printf("ID: %d, Name: %s, Date: %s, Venue: %s, Performer ID: %d, Organizer ID: %d\n",
                            id, name, date.toString(), venue, performerIdFromDb, organizerId);
                }
            }
        }
    }

    //    7 запрос

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


    public static void getAllTicketsByDate(Connection connection, Date purchaseDate) throws SQLException {
        String query = "SELECT * FROM ticket WHERE purchase_date = ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setDate(1, purchaseDate);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    String ticketNumber = resultSet.getString("ticket_number");
                    int concertId = resultSet.getInt("concert_id");
                    Date purchaseDateFromDb = resultSet.getDate("purchase_date");
                    double price = resultSet.getDouble("price");

                    System.out.printf("ID: %d, Ticket Number: %s, Concert ID: %d, Purchase Date: %s, Price: %.2f\n",
                            id, ticketNumber, concertId, purchaseDateFromDb.toString(), price);
                }
            } catch (Exception e){
                System.out.println(e.getMessage());
            }
        }
    }

    //    9 запрос

    public static void getAllPerformersByCountry(Connection connection, String country) throws SQLException {
        String query = "SELECT * FROM performer WHERE country = ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, country);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    String name = resultSet.getString("name");
                    String genre = resultSet.getString("genre");
                    String countryFromDb = resultSet.getString("country");

                    System.out.printf("ID: %d, Name: %s, Genre: %s, Country: %s: \n",
                            id, name, genre, countryFromDb);
                }
            }
        }
    }

    //    10 запрос

    public static void getAllTicketsAbovePrice(Connection connection, double price) throws SQLException {
        String query = "SELECT * FROM ticket WHERE price > ?";

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

    //    11 запрос

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

    //    12 запрос

    public static void getAllConcertsBelowTicketPrice(Connection connection, double price) throws SQLException {
        String query = "SELECT * FROM concert INNER JOIN ticket ON concert.id = ticket.concert_id WHERE ticket.price < ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setDouble(1, price);

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
        } catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    //    13 запрос

    public static void getAllOrganizersByContactNumber(Connection connection, String contactNumber) throws SQLException {
        String query = "SELECT * FROM organizer WHERE contact_phone = ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, contactNumber);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    String name = resultSet.getString("name");
                    String contactPhone = resultSet.getString("contact_phone");
                    String email = resultSet.getString("email");

                    System.out.printf("ID: %d, Name: %s, Contact Phone: %s, Email: %s: \n",
                            id, name, contactPhone, email);
                }
            } catch (Exception e){
                System.out.println(e.getMessage());
            }
        }
    }

    //    14 запрос

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
