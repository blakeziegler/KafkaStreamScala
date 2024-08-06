import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.compiletime.ops.double
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.parser.decode
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.Serdes
import io.circe.Encoder
import io.circe.Decoder
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.kstream.JoinWindows
import java.time.temporal.ChronoUnit
import java.time.Duration
import org.apache.kafka.streams.StreamsConfig


def main(args: Array[String]): Unit = {
  // create client/user profile
  object client {
    type User = String
    type Profile = String
    type Product = String
    type OrderId = String
    type Status = String

    // client classes: Orders, Payment, Discount
    case class Order(orderid: OrderId, user: User, products: List[Product], amount: BigDecimal)
    case class Payment(orderid: OrderId, status: Status)
    case class Discount(profile: Profile, amount: BigDecimal)
    
  }
  // data stream topics
  object Topics {
    val OrdersByUser = "orders-by-user"
    val DiscountProfiles = "discount-profiles"
    val Discounts = "discounts"
    val Orders = "orders"
    val Payments = "payments"
    val Paid = "paid"

    // Repo CLI:
    // docker exec -it redpanda-0 bash
    // bash$ rpk topic create 'topic name'
  }
  
  // import needed objs
  import client._
  import Topics._

  // Transform data and build streams w/ Kafka's Serde import
  implicit def serde[A >: Null : Decoder: Encoder]: Serde[A] = {
    // Raw information -> bytes -> byte array -> byte string -> decoded string
    val sterializer = (a : A) => a.asJson.noSpaces.getBytes()
    val desteralizer = (bytes: Array[Byte]) => {
      val string = new String(bytes)
      decode[A](string).toOption
    }

    Serdes.fromFn[A](sterializer, desteralizer)
    
  }

  // Define builder to build Kafka Streams
  val builder = new StreamsBuilder()

  // Build Kafka Stream
  val userOrdersStream: KStream[User, Order] = 
    builder.stream[User, Order](OrdersByUser)

  // Build K Table
  val userprofilesTable: KTable[User, Profile] = 
    builder.table[User, Profile](DiscountProfiles)

  // Repo CLI:
  // docker exec -it redpanda-0 bash
  // bash$ rpk topic delete discount-profiles
  // bash$ rpk topic create discount-profiles --config "cleanup.policy=compact"

  // Global K Table
  val discountProfilesGlobal: GlobalKTable[Profile, Discount] = 
    builder.globalTable[Profile, Discount](Discounts)

  // Repo CLI:
  // docker exec -it redpanda-0 bash
  // bash$ rpk topic delete discounts
  // bash$ rpk topic create discounts --config "cleanup.policy=compact"
  
  // Kstream transformation
  // filter, map, flatmap
  val expensiveOrders = userOrdersStream.filter { (user, order) =>
    order.amount > 1000
  }

  val listOfProducts = userOrdersStream.mapValues { order =>
    order.products
  }

  val productsStream = userOrdersStream.flatMapValues(_.products)

  // Join Ktables
  val ordersWithProfiles = userOrdersStream.join(userprofilesTable) { (order, profile) =>
    (order, profile)
  }
  
  // Join the order profile with the profile specific discount
  // Key: OderProfile; Value: Discount Amount
  val discountedOrderStream = ordersWithProfiles.join(discountProfilesGlobal) (
    { case (user, (order,profile)) => profile },
    { case ((order, profile), discount) =>
    // Return new order with applied discount
      order.copy(amount = order.amount - discount.amount) }   
  )
  // Set order and payment streams
  val orderStream = discountedOrderStream.selectKey((user, order) => order.orderid)
  val paymentsGlobalTable: GlobalKTable[OrderId, Payment] = builder.globalTable[OrderId, Payment](Payments)
  // Join Window: Allows for time differences between keys and values
  // Window length: 30 Seconds
  val joinWindow = JoinWindows.of(Duration.of(30, ChronoUnit.SECONDS))
  // Identify only paid orders:
  val joinOrdersPayments = (order: Order, payment: Payment) =>
    if (payment.status == "PAID") Option(order) else Option.empty[Order]
  // Within the paymentStream, identify paid orders and apply the join window
  val ordersPaid = orderStream.join[OrderId, Payment, Option[Order]](
    paymentsGlobalTable
  )(
    { case (orderId, order) => orderId },
    joinOrdersPayments
  ).flatMapValues {
    case None => List.empty[Order]
    case Some(order) => List(order)
  }
  
  val topology = builder.build()

  val props = new Properties
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Order-Tracker")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8080")
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)

  val app = new KafkaStreams(topology, props)
  app.start()

}
