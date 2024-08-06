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

object data {
  object client {
    type User = String
    type Profile = String
    type Product = String
    type OrderId = String
    type Status = String

    case class Order(orderid: OrderId, user: User, products: List[Product], amount: Double)
    case class Payment(orderid: OrderId, status: Status, amount:BigDecimal)
    case class Discount(profile: Profile, amount: Double)
    
  }
  
  object Topics {
    val OrdersByUser = "orders-by-user"
    val DiscountProfiles = "discount-profiles"
    val Discounts = "discounts"
    val Orders = "orders"
    val Payments = "payments"
    val Paid = "paid"

  }

  import client._
  import Topics._
  implicit def serde[A >: Null : Decoder: Encoder]: Serde[A] = {
    val sterializer = (a : A) => a.asJson.noSpaces.getBytes()
    val desteralizer = (bytes: Array[Byte]) => {
      val string = new String(bytes)
      decode[A](string).toOption
    }

    Serdes.fromFn[A](sterializer, desteralizer)
    
  }

  val builder = new StreamsBuilder()
  val userOrdersStream: KStream[User, Order] = builder.stream[User, Order](OrdersByUser)
  val profilesTable: KTable[User, Profile] = builder.table[User, Profile](DiscountProfiles)

  def main(args: Array[String]): Unit = {
  }
}
