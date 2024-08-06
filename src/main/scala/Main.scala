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


// define data obj
object data {
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
    case class Discount(profile: Profile, amount: Double)
    
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
    // Raw information -> bytes -> byte array -> string -> decoded string
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
  // filter, map
  val expensiveOrders = userOrdersStream.filter { (user, order) =>
    order.amount > 1000
  }

  val listOfProducts = userOrdersStream.mapValues { order =>
    order.products
  }


  def main(args: Array[String]): Unit = {
  }
}
