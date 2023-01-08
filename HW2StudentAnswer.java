package bigdatacourse.hw2.studentcode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.Buffer;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import com.datastax.oss.driver.api.core.CqlSession;

import bigdatacourse.hw2.HW2API;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


public class HW2StudentAnswer implements HW2API {

    // general consts
    public static final String NOT_AVAILABLE_VALUE = "na";
    private static final String TABLE_ITEMS = "items";
    private static final String TABLE_USER_REVIEWS = "user_reviews";
    private static final String TABLE_ITEM_REVIEWS = "item_reviews";

    private static final String TABLE_USER_REVIEWS_COUNT = "user_reviews_count";

    private static final String TABLE_ITEM_REVIEWS_COUNT = "item_reviews_count";

    // CQL stuff
    private static final String CQL_CREATE_ITEM_TABLE =
            "CREATE TABLE " + TABLE_ITEMS + "(" +
                    "asin text," +
                    "title text," +
                    "image text," +
                    "categories Set<text>," +
                    "description text," +
                    "PRIMARY KEY (asin)" +
                    ") ";
    private static final String CQL_CREATE_USER_REVIEWS_TABLE =
            "CREATE TABLE " + TABLE_USER_REVIEWS + "(" +
                    "time timestamp," +
                    "asin text," +
                    "reviewerID text," +
                    "reviewerName text," +
                    "rating int," +
                    "summary text," +
                    "reviewText text," +
                    "PRIMARY KEY ((reviewerID), time, asin)" +
                    ") " +
                    "WITH CLUSTERING ORDER BY (time DESC, asin ASC)";
    private static final String CQL_CREATE_USER_REVIEWS_COUNT_TABLE =
            "CREATE TABLE " + TABLE_USER_REVIEWS_COUNT + "(" +
                    "reviewerID text," +
                    "countByReviewerID COUNTER," +
                    "PRIMARY KEY ((reviewerID))" +
                    ") ";

    private static final String CQL_CREATE_ITEM_REVIEWS_TABLE =
            "CREATE TABLE " + TABLE_ITEM_REVIEWS + "(" +
                    "time timestamp," +
                    "asin text," +
                    "reviewerID text," +
                    "reviewerName text," +
                    "rating int," +
                    "summary text," +
                    "reviewText text," +
                    "PRIMARY KEY ((asin), time, reviewerID)" +
                    ") " +
                    "WITH CLUSTERING ORDER BY (time DESC, reviewerID ASC)";
    private static final String CQL_CREATE_ITEM_REVIEWS_COUNT_TABLE =
            "CREATE TABLE " + TABLE_ITEM_REVIEWS_COUNT + "(" +
                    "asin text," +
                    "countByAsin COUNTER," +
                    "PRIMARY KEY ((asin))" +
                    ") ";

    // cassandra session
    private CqlSession session;

    // prepared statements
    private static final String CQL_ITEM_INSERT =
            "INSERT INTO " + TABLE_ITEMS + "(asin, title, image, categories, description) VALUES(?, ?, ?, ?, ?)";

    private static final String CQL_USER_REVIEWS_INSERT =
            "INSERT INTO " + TABLE_USER_REVIEWS + "(time, asin, reviewerID, reviewerName, rating, summary, reviewText) VALUES(?, ?, ?, ?, ?, ?, ?)";

    private static final String CQL_USER_REVIEWS_COUNT_UPDATE =
            "UPDATE " + TABLE_USER_REVIEWS_COUNT + " SET countByReviewerID = countByReviewerID + 1 WHERE reviewerID = ?";

    private static final String CQL_ITEM_REVIEWS_INSERT =
            "INSERT INTO " + TABLE_ITEM_REVIEWS + "(time, asin, reviewerID, reviewerName, rating, summary, reviewText) VALUES(?, ?, ?, ?, ?, ?, ?)";

    private static final String CQL_ITEM_REVIEWS_COUNT_UPDATE =
            "UPDATE " + TABLE_ITEM_REVIEWS_COUNT + " SET countByAsin = countByAsin + 1 WHERE asin = ?";

    private static final String CQL_ITEM_SELECT = "SELECT asin, title, image, categories, description FROM " + TABLE_ITEMS + " WHERE asin=?";

    private static final String CQL_USER_REVIEWS_SELECT = "SELECT time, asin, reviewerID, reviewerName, rating, summary, reviewText FROM " + TABLE_USER_REVIEWS + " WHERE reviewerID=?";

    private static final String CQL_USER_REVIEWS_COUNT_SELECT = "SELECT reviewerID, countByReviewerID FROM " + TABLE_USER_REVIEWS_COUNT + " WHERE reviewerID=?";

    private static final String CQL_ITEM_REVIEWS_SELECT = "SELECT time, asin, reviewerID, reviewerName, rating, summary, reviewText FROM " + TABLE_ITEM_REVIEWS + " WHERE asin=?";

    private static final String CQL_ITEM_REVIEWS_COUNT_SELECT = "SELECT asin, countByAsin FROM " + TABLE_ITEM_REVIEWS_COUNT + " WHERE asin=?";

    PreparedStatement pstmt_insert_item;
    PreparedStatement pstmt_select_item;

    PreparedStatement pstmt_insert_user_reviews;
    PreparedStatement pstmt_select_user_reviews;

    PreparedStatement pstmt_update_user_reviews_count;
    PreparedStatement pstmt_select_user_reviews_count;

    PreparedStatement pstmt_insert_item_reviews;
    PreparedStatement pstmt_select_item_reviews;

    PreparedStatement pstmt_update_item_reviews_count;
    PreparedStatement pstmt_select_item_reviews_count;
    static int count = 0;

    @Override
    public void connect(String pathAstraDBBundleFile, String username, String password, String keyspace) {
        if (session != null) {
            System.out.println("ERROR - cassandra is already connected");
            return;
        }

        System.out.println("Initializing connection to Cassandra...");

        this.session = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(pathAstraDBBundleFile))
                .withAuthCredentials(username, password)
                .withKeyspace(keyspace)
                .build();

        System.out.println("Initializing connection to Cassandra... Done");
    }

    @Override
    public void close() {
        if (session == null) {
            System.out.println("Cassandra connection is already closed");
            return;
        }

        System.out.println("Closing Cassandra connection...");
        session.close();
        System.out.println("Closing Cassandra connection... Done");
    }

    @Override
    public void createTables() {
        session.execute(CQL_CREATE_ITEM_TABLE);
        session.execute(CQL_CREATE_USER_REVIEWS_TABLE);
        session.execute(CQL_CREATE_USER_REVIEWS_COUNT_TABLE);
        session.execute(CQL_CREATE_ITEM_REVIEWS_TABLE);
        session.execute(CQL_CREATE_ITEM_REVIEWS_COUNT_TABLE);
    }

    @Override
    public void initialize() {
        pstmt_insert_item = session.prepare(CQL_ITEM_INSERT);
        pstmt_select_item = session.prepare(CQL_ITEM_SELECT);

        pstmt_insert_user_reviews = session.prepare(CQL_USER_REVIEWS_INSERT);
        pstmt_select_user_reviews = session.prepare(CQL_USER_REVIEWS_SELECT);

        pstmt_update_user_reviews_count = session.prepare(CQL_USER_REVIEWS_COUNT_UPDATE);
        pstmt_select_user_reviews_count = session.prepare(CQL_USER_REVIEWS_COUNT_SELECT);

        pstmt_insert_item_reviews = session.prepare(CQL_ITEM_REVIEWS_INSERT);
        pstmt_select_item_reviews = session.prepare(CQL_ITEM_REVIEWS_SELECT);

        pstmt_update_item_reviews_count = session.prepare(CQL_ITEM_REVIEWS_COUNT_UPDATE);
        pstmt_select_item_reviews_count = session.prepare(CQL_ITEM_REVIEWS_COUNT_SELECT);
    }

    public void insert_item(String asin, String title, String image, TreeSet<String> categories, String description) {
        BoundStatement bstmt = pstmt_insert_item.bind()
                .setString(0, asin)
                .setString(1, title)
                .setString(2, image)
                .setSet(3, categories, String.class)
                .setString(4, description);
        session.execute(bstmt);
    }

    public void insert_by_review(PreparedStatement pstmt, Long time, String asin, String reviewerID, String reviewerName, int rating, String summary, String reviewtext) {
        BoundStatement bstmt = pstmt.bind()
                .setInstant(0, Instant.ofEpochSecond(time))
                .setString(1, asin)
                .setString(2, reviewerID)
                .setString(3, reviewerName)
                .setInt(4, rating)
                .setString(5, summary)
                .setString(6, reviewtext);
        session.execute(bstmt);
    }

    public void update_count(PreparedStatement pstmt, String key) {
        BoundStatement bstmt = pstmt.bind()
                .setString(0, key);
        session.execute(bstmt);
    }

    public String getStringOrNAV(JSONObject object, String key) {
        if (!object.has(key)) return NOT_AVAILABLE_VALUE;
        return object.getString(key);
    }

    public TreeSet<String> getCategoriesArray(JSONObject object) {
        TreeSet<String> s = new TreeSet<>();
        JSONArray arr = object.getJSONArray("categories");
        for (int i = 0; i < arr.length(); i++) {
            JSONArray inner_arr = arr.getJSONArray(i);
            for (int j = 0; j < inner_arr.length(); j++) s.add(inner_arr.getString(j));
        }
        return s;
    }

    @Override
    public void loadItems(String pathItemsFile) throws Exception {
        Consumer<JSONObject>[] f;
        Consumer<JSONObject> func;
        func = (item) -> {
            insert_item(item.getString("asin"),
                    getStringOrNAV(item, "title"),
                    getStringOrNAV(item, "imUrl"),
                    getCategoriesArray(item),
                    getStringOrNAV(item, "description"));
        };
        f = new Consumer[]{func};
        load(pathItemsFile, f);
    }

    public void load(String pathReviewsFile, Consumer<JSONObject>[] f) throws Exception {
        int i;
        FileReader reader = new FileReader(pathReviewsFile);
        BufferedReader buffer = new BufferedReader(reader);
        ExecutorService executor = Executors.newFixedThreadPool(250);
        while (buffer.ready()) {
            JSONObject item = new JSONObject(buffer.readLine());
            for (i = 0; i < f.length; i++) {
                int finalI = i;
                executor.execute(() -> f[finalI].accept(item));
            }
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);
    }

    @Override
    public void loadReviews(String pathReviewsFile) throws Exception {
        Consumer<JSONObject> f1, f2, f3, f4;
        Consumer<JSONObject>[] round_1, round_2;
        f1 = (item) -> {
            insert_by_review(pstmt_insert_user_reviews,
                    item.getLong("unixReviewTime"),
                    getStringOrNAV(item, "asin"),
                    item.getString("reviewerID"),
                    getStringOrNAV(item, "reviewerName"),
                    item.getInt("overall"),
                    getStringOrNAV(item, "summary"),
                    getStringOrNAV(item, "reviewText"));
//            System.out.println(count++);
        };
        f2 = (item) -> {
            update_count(pstmt_update_user_reviews_count, item.getString("reviewerID"));
        };
        f3 = (item) -> {
            insert_by_review(pstmt_insert_item_reviews,
                    item.getLong("unixReviewTime"),
                    item.getString("asin"),
                    getStringOrNAV(item, "reviewerID"),
                    getStringOrNAV(item, "reviewerName"),
                    item.getInt("overall"),
                    getStringOrNAV(item, "summary"),
                    getStringOrNAV(item, "reviewText"));
//            System.out.println(count++);
        };
        f4 = (item) -> {
            update_count(pstmt_update_item_reviews_count, item.getString("asin"));
        };

        round_1 = new Consumer[]{f1, f2};
        round_2 = new Consumer[]{f3, f4};
        load(pathReviewsFile, round_1);
        count = 0;
        load(pathReviewsFile, round_2);
    }

    @Override
    public void item(String asin) {
        BoundStatement bstmt = pstmt_select_item.bind()
                .setString(0, asin);
        ResultSet rs = session.execute(bstmt);
        Row row = rs.one();
        if (row == null) System.out.println("not exists");
        else {
            Set<String> mySet = row.getSet(3, String.class);
            System.out.println("asin: " + row.getString(0));
            System.out.println("title: " + row.getString(1));
            System.out.println("image: " + row.getString(2));
            if (mySet != null) System.out.println("categories: " + mySet);
            System.out.println("description: " + row.getString(4));
        }
    }

    @Override
    public void userReviews(String reviewerID) {
        // the order of the reviews should be by the time (desc), then by the asin
        BoundStatement bstmt1 = pstmt_select_user_reviews.bind()
                .setString(0, reviewerID);
        BoundStatement bstmt2 = pstmt_select_user_reviews_count.bind()
                .setString(0, reviewerID);
        ResultSet rs1 = session.execute(bstmt1);
        ResultSet rs2 = session.execute(bstmt2);

        Row row = rs1.one();

        while (row != null) {
            System.out.println(
                    "time: " + row.getInstant(0).toString() +
                    ", asin: " + row.getString(1) +
                            ", reviewerID: " + row.getString(2) +
                            ", reviewerName: " + row.getString(3) +
                            ", rating: " + row.getInt(4) +
                            ", summary: " + row.getString(5) +
                            ", reviewText: " + row.getString(6));
            row = rs1.one();
        }
        row = rs2.one();
        if (row != null) System.out.println("total reviews: " + row.getLong(1));
        else System.out.println("total reviews: " + 0);
    }

    @Override
    public void itemReviews(String asin) {
        // the order of the reviews should be by the time (desc), then by the reviewerID
        BoundStatement bstmt1 = pstmt_select_item_reviews.bind()
                .setString(0, asin);
        BoundStatement bstmt2 = pstmt_select_item_reviews_count.bind()
                .setString(0, asin);
        ResultSet rs1 = session.execute(bstmt1);
        ResultSet rs2 = session.execute(bstmt2);

        Row row = rs1.one();

        while (row != null) {
            System.out.println(
                    "time: " + row.getInstant(0).toString() +
                    ", asin: " + row.getString(1) +
                            ", reviewerID: " + row.getString(2) +
                            ", reviewerName: " + row.getString(3) +
                            ", rating: " + row.getInt(4) +
                            ", summary: " + row.getString(5) +
                            ", reviewText: " + row.getString(6));
            row = rs1.one();
        }
        row = rs2.one();
        if (row != null) System.out.println("total reviews: " + row.getLong(1));
        else System.out.println("total reviews: " + 0);
    }
}
