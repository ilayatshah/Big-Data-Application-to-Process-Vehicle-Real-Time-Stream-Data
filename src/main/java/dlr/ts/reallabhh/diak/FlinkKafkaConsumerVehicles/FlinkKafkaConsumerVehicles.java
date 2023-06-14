package dlr.ts.reallabhh.diak.FlinkKafkaConsumerVehicles;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.hbase.client.Connection;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.util.factory.Hints;
import org.json.JSONArray;
import org.json.JSONObject;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;

public class FlinkKafkaConsumerVehicles {

	public static void main(String[] args) throws Exception {

		// fetch runtime arguments
		String bootstrapServers = "10.21.129.123:9092";
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Set up the Consumer and create a datastream from this source
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", bootstrapServers);
		properties.setProperty("group.id", "diak_vehicles");
		FlinkKafkaConsumer<String> flinkConsumer = new FlinkKafkaConsumer<>("diak_vehicles", // input topic
				new SimpleStringSchema(), // serialization schema
				properties); // properties

		flinkConsumer.setStartFromTimestamp(Long.parseLong("0"));

		DataStream<String> readingStream = env.addSource(flinkConsumer);
		readingStream.rebalance().map(new RichMapFunction<String, String>() {

			private static final long serialVersionUID = -2547861355L; // random number

			// Define all the global variables here which will be used in this application
			// variables to store Kafka msg processing summary
			private long numberOfMessagesProcessed;
			private long numberOfMessagesFailed;
			private long numberOfMessagesSkipped;

			// Define the variables for both the Live and History tables in which the data
			// will be store in the HBase
			DataStore vehicles_live = null;
			DataStore vehicles_history = null;

			SimpleFeatureType sft_live;
			SimpleFeatureType sft_history;
			SimpleFeatureBuilder SFbuilderHist; // feature builder for history
			SimpleFeatureBuilder SFbuilderLive; // feature builder for live

			List<SimpleFeature> vehicles_history_features; // Features list of SimpleFeature-s to store
															// diak:vehicles_history messages
			List<SimpleFeature> vehicles_live_features; // Features list of SimpleFeature-s to store diak:vehicles_live
														// messages

			@Override
			public void open(Configuration parameters) throws Exception {
				System.out.println("In open method.");

				// --- GEOMESA, GEOTOOLS APPROACH ---//
				// define connection parameters to diak:vehicles_live GeoMesa-HBase DataStore
				Map<String, Serializable> params_live = new HashMap<>();
				params_live.put("hbase.catalog", "diak:vehicles_live"); // HBase table name
				params_live.put("hbase.zookeepers",
						"ts-dlr-bs,ts-ts-dlr-bs"); // address of the hbase zookeeper (dummy here)

				try {
					vehicles_live = DataStoreFinder.getDataStore(params_live);
					if (vehicles_live == null) {
						System.out.println("Could not connect to diak:vehicles_live");
					} else {
						System.out.println("Successfully connected to diak:vehicles_live");
					}
				} catch (IOException e) {
					e.printStackTrace();
				}

				// create simple feature type for diak:vehicle_live_table which will have the
				// following columns elements in the table
				StringBuilder attributes = new StringBuilder();
				attributes.append("timestamp:Long,");
				attributes.append("source:String,");
				attributes.append("vehicleType:String,");
				attributes.append("speed:Double,");
				attributes.append("*vehiclePosition:Point:srid=4326");
				// a table template will be created with the name, "diak:vehicles_live_table", 
				// in which all the above columns will be added
				sft_live = SimpleFeatureTypes.createType("diak_vehicles_live", attributes.toString());

				// define connection parameters to diak:vehicles_history GeoMesa-HBase DataStore
				Map<String, Serializable> params_his = new HashMap<>();
				params_his.put("hbase.catalog", "diak:vehicles_history"); // HBase table name
				params_his.put("hbase.zookeepers",
						"ts-dlr-bs,ts-ts-dlr-bs"); // address of the hbase zookeeper (dummy here)

				try {
					vehicles_history = DataStoreFinder.getDataStore(params_his);
					if (vehicles_history == null) {
						System.out.println("Could not connect to diak:vehicles_history");
					} else {
						System.out.println("Successfully connected to diak:vehicles_history");
					}
				} catch (IOException e) {
					e.printStackTrace();
				}

				// create simple feature type for diak_vehicle_history_table which will have the
				// following columns elements in the table
				StringBuilder attributes1 = new StringBuilder();
				attributes1.append("vehicleID:String,");
				attributes1.append("timestamp:Long,");
				attributes1.append("source:String,");
				attributes1.append("vehicleType:String,");
				attributes1.append("speed:Double,");
				attributes1.append("*vehiclePosition:Point:srid=4326");
				// a table structure will be created with the name,
				// "diak:vehicles_history_table", in which all the above columns will be added
				sft_history = SimpleFeatureTypes.createType("diak_vehicles_history", attributes1.toString());

				try {
					vehicles_history.createSchema(sft_history); // created if it doesn't exist. If not, execution of
																// this statement does nothing
					vehicles_live.createSchema(sft_live); // created if it doesn't exist. If not, execution of this
															// statement does nothing
				} catch (IOException e) {
					e.printStackTrace();
				}

				//initialise variables
				numberOfMessagesProcessed = 0;
				numberOfMessagesFailed = 0;
				numberOfMessagesSkipped = 0;

				//for Vehicles_Live
				vehicles_live_features = new ArrayList<>();
				SFbuilderLive = new SimpleFeatureBuilder(sft_live);

				// for Vehicles_History
				vehicles_history_features = new ArrayList<>();
				SFbuilderHist = new SimpleFeatureBuilder(sft_history);
			}

			public String map(String valueFromKafka) throws Exception {
				// this function runs for every message from kafka topic

				// check if the message has any content
				if (valueFromKafka == null || valueFromKafka.trim().isEmpty()) {
					System.out.println("Message from Kafka is empty!");
					numberOfMessagesSkipped++;
					return "Warning: Message from Kafka is empty";
				} else {
					try {

						// convert JSON String (kafka messages) into JSON Object
						// to extract rowid (combination of vehicle ID and time stamp of the message)
						JSONObject json_obj = new JSONObject(valueFromKafka);
						System.out.println(json_obj.toString());

						// define variables to extract the values from the json_obj into the HBase
						// vehicles_history table
						// get value for a key
						String rowidHistory = json_obj.getString("featureOfInterest");
						String[] splitObj = rowidHistory.split("-"); // split the feature of interest as we have 3
																		// features there
						String vehicleID = splitObj[1] + splitObj[2]; // at index 2 (1) we have vehicle id
						String vehicleType = splitObj[0]; // at index 1 (0) we have vehicle type

						//phenomenonTime = resultTime for messages from simulation
						// convert ISO datetime string into unixtime/epochseconds
						String timestamp_ISO = json_obj.getString("resultTime");
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
						Date dt = sdf.parse(timestamp_ISO);
						long timestamp = dt.getTime() / 1000;

						JSONArray speedList = (JSONArray) json_obj.getJSONArray("member");
						Double speed = speedList.getJSONObject(1).getJSONObject("result").getDouble("value");
						JSONArray coordJson = speedList.getJSONObject(0).getJSONObject("result")
								.getJSONArray("coordinates");
						String pointWKT = "POINT(" + coordJson.getFloat(0) + " " + coordJson.getFloat(1) + ")";

						// create and add feature to the vehicle history SFbuilder
						SFbuilderHist.set("vehicleID", vehicleID);
						SFbuilderHist.set("vehicleType", vehicleType);
						SFbuilderHist.set("timestamp", timestamp);
						SFbuilderHist.set("speed", speed);
						SFbuilderHist.set("source", "SIMULATION"); //shan_sa
						SFbuilderHist.set("vehiclePosition", pointWKT);
						SFbuilderHist.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);
						vehicles_history_features = new ArrayList<>();
						vehicles_history_features.add(SFbuilderHist.buildFeature(rowidHistory));

						try (FeatureWriter<SimpleFeatureType, SimpleFeature> writerHistory = vehicles_history
								.getFeatureWriterAppend(sft_history.getTypeName(), Transaction.AUTO_COMMIT)) {
							System.out.println("Writing " + vehicles_history_features.size()
									+ " features to diak:vehicles_history");
							for (SimpleFeature feature : vehicles_history_features) {
								SimpleFeature toWrite = writerHistory.next();
								toWrite.setAttributes(feature.getAttributes());
								((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
								toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
								toWrite.getUserData().putAll(feature.getUserData());
								writerHistory.write();
							}
						} catch (IOException e) {
							e.printStackTrace();
						}

						// delete row if already present in table
						FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);
						Filter filter = ff.id(Collections.singleton(ff.featureId(vehicleID)));
						SimpleFeatureStore featurestore_live = (SimpleFeatureStore) vehicles_live
								.getFeatureSource("diak_vehicles_live");
						featurestore_live.removeFeatures(filter);

						// create and add feature to the vehicle live SFbuilder
						SFbuilderLive.set("vehicleType", vehicleType);
						SFbuilderLive.set("timestamp", timestamp);
						SFbuilderLive.set("speed", speed);
						SFbuilderLive.set("source", "SIMULATION"); //shan_sa
						SFbuilderLive.set("vehiclePosition", pointWKT);
						SFbuilderLive.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);
						vehicles_live_features = new ArrayList<>();
						vehicles_live_features.add(SFbuilderLive.buildFeature(vehicleID));

						/*
						 * push the newly created feature to a local list, push the entire list into the
						 * HBase table by copying the list into a local variable and empty the list for
						 * the next iteration
						 */

						try (FeatureWriter<SimpleFeatureType, SimpleFeature> writerLive = vehicles_live
								.getFeatureWriterAppend(sft_live.getTypeName(), Transaction.AUTO_COMMIT)) {
							System.out.println(
									"Writing " + vehicles_live_features.size() + " features to diak:vehicles_live");
							for (SimpleFeature feature : vehicles_live_features) {
								SimpleFeature toWrite = writerLive.next();
								toWrite.setAttributes(feature.getAttributes());
								((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
								toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
								toWrite.getUserData().putAll(feature.getUserData());
								writerLive.write();
								numberOfMessagesProcessed++; //shan_sa
							}
						} catch (Exception e) {
							e.printStackTrace();

						}
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println(valueFromKafka); //shan_sa
						numberOfMessagesFailed++; //shan_sa
					}

				}
				return "success";
			}

			@Override
			public void close() throws Exception {
				System.out.println("In close function!");
				// this function runs when the flink job stops/is stopped

				// close datastore and connection
				if (vehicles_live != null)
					vehicles_live.dispose();
				if (vehicles_history != null)
					vehicles_history.dispose();
				//connection.close(); //shan_sa: object connection is undefined and unused
				System.out.println("Connections to HBase Datastore successfully closed");

				// Print processing stats
				System.out.println(flinkConsumer.toString() + " Number of messages successfully processed: "
						+ numberOfMessagesProcessed);
				System.out.println(flinkConsumer.toString() + " Number of messages failed: " + numberOfMessagesFailed);
				System.out
						.println(flinkConsumer.toString() + " Number of messages skipped: " + numberOfMessagesSkipped);

			}

		});

		env.execute("dlr.ts.reallabhh.diak.FlinkKafkaConsumerVehicles");
	}
}
