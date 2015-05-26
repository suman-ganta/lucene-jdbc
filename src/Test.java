import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.uninverting.UninvertingReader;

import java.util.HashMap;


public class Test {

    public static void main(String[] args) throws Exception {
        Directory directory = new RAMDirectory();
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        IndexWriter indexWriter = new IndexWriter(directory, iwc);
        Document doc1 = buildDocument("DATA:somedata", "ACL_FIELD:access");
        indexWriter.addDocument(doc1);
        Document doc2 = buildDocument("DATA:somedata", "ACL_FIELD:noaccess");
        indexWriter.addDocument(doc2);

        //update the index
        indexWriter.updateDocument(new Term("ACL_FIELD", "access"), buildDocument("DATA:hello", "ACL_FIELD:access"));
        indexWriter.close();


        DirectoryReader reader = DirectoryReader.open(directory);
        HashMap<String, UninvertingReader.Type> mapping = new HashMap<String, UninvertingReader.Type>();
        mapping.put("ACL_FIELD", UninvertingReader.Type.SORTED);
        DirectoryReader ureader = UninvertingReader.wrap(reader, mapping);

        IndexSearcher indexSearcher = new IndexSearcher(ureader);

        QueryParser parser = new QueryParser("DATA", analyzer);
        Query query = parser.parse("hello");

        //Filter based on acl field
        Filter aclFilter = null;
        aclFilter = new DocValuesTermsFilter("ACL_FIELD", "access");

        ScoreDoc[] hits = indexSearcher.search(query, aclFilter, 100).scoreDocs;
        System.out.println("Hits[" + hits.length + "]");
        for (int i = 0; i < hits.length; i++) {
            Document doc = indexSearcher.doc(hits[i].doc);
            System.out.println("DATA [" + doc.get("DATA") + "] ACL_FIELD [" + doc.get("ACL_FIELD") + "]");
        }
        reader.close();
    }

    private static Document buildDocument(String... fieldInfo) {
        Document document = new Document();
        for (int i = 0; i < fieldInfo.length; i++) {
            String[] split = fieldInfo[i].split(":");
            String fieldName = split[0];
            String fieldValue = split[1];
            Field field = new Field(fieldName, fieldValue, TextField.TYPE_STORED);
            document.add(field);
        }
        return document;
    }
}