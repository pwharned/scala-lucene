import database.Configuration
import entitites.{CodeResult, SimpleCodeResult}
import logging.Logging
import org.apache.lucene.analysis.core.SimpleAnalyzer
import org.apache.lucene.document.{Document, Field, StoredField, TextField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.ByteBuffersDirectory
import org.apache.lucene.index.Term
import org.apache.lucene.search.FuzzyQuery
import org.apache.lucene.search.spans.{SpanMultiTermQueryWrapper, SpanNearQuery, SpanQuery}
import java.io.{BufferedInputStream, FileInputStream}
import java.util.zip.GZIPInputStream
import scala.collection.parallel.CollectionConverters.IterableIsParallelizable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration.Inf
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.util.{Failure, Success, Try}
  
object DocumentIndex extends App{

  val directory = new ByteBuffersDirectory

  val writer = new IndexWriter(directory, new IndexWriterConfig(new SimpleAnalyzer))

  def addDocument(code: String, str: String, preferred_term: String, writer: IndexWriter): Future[Long] = Future{
    val doc = new Document

    doc.add(new StoredField("code", code))
    doc.add(new StoredField("preferred_term", preferred_term))

    doc.add(new TextField("str", str, Field.Store.YES))

    writer.addDocument(doc)

  }
  
  
  def index: Future[Seq[Unit]] = {




    val path = 'f1.tar.gz:f2.tar.gz'


    val paths = path.split(":")
    paths.foreach(println)
    val iterators = paths.map(x => Future(Source.fromInputStream(gis(x)))).toSeq
    val result: Seq[Future[Unit]] = iterators.map(iterator => iterator.map(it => it.getLines.grouped(chunkSize).foreach { lines =>
      lines.par.foreach { line =>
        val sl = line.split("\\|")

        Try(addDocument(sl.head, sl.last.toLowerCase, sl(14), writer)).recover(x => {println(sl.mkString("|")); println(x.toString)})

      }
    }
    )
    )

    Future.sequence(result)

  }
  
  def gis(s: String) = new GZIPInputStream(new BufferedInputStream(new FileInputStream(s)))
  
    def indexAsync: Unit = {
    val start = System.currentTimeMillis()
    index.onComplete {
      case Success(value) => info(f"Finished indexing ${writer.getDocStats.numDocs} in ${(System.currentTimeMillis() - start) / 1000} seconds "); writer.commit()
      case Failure(exception) => error(exception.toString)

    }
  }
  
  

}
