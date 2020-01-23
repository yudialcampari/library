package org.lskk.lumen.wahyu.GPAR;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.springframework.util.StreamUtils.BUFFER_SIZE;

public class graphPatternAS {
    GraphDatabaseService graphDb;
    private static String prop1[] = new String[6000];

    public void ARPolaSatu(File pathdb, File input, File output) throws IOException, InterruptedException {

        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(pathdb);
        final List<String[]> listnode = new ArrayList<>();
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(input), StandardCharsets.UTF_8), BUFFER_SIZE);
             final CSVReader csv = new CSVReader(reader)) {
            String[] nextLine;

            while ((nextLine = csv.readNext()) != null) {
                if (nextLine[0].startsWith("Relasi")) {
                    continue;
                }
                String rel1 = nextLine[0];
                String countHead = nextLine[1];
                //pola 1 dan 2 tinggal diubah panah  untuk r2
                String Query = "MATCH (y:owl_Thing)<-[r{rdf_type:'" + rel1 + "'}]-(x:owl_Thing)<-[r2]-(y:owl_Thing) RETURN DISTINCT \"relationship\" AS element, \n" +
                        "r2.rdf_type As rel2,count (distinct x.nn) AS subjek order by subjek desc";

                Map<String, Object> params = new HashMap<String, Object>();
                try (Transaction tx = graphDb.beginTx();
                     Result result = graphDb.execute(Query, params)) {
                    while (result.hasNext()) {
                        final Map<String, Object> row = result.next();
                        // final String rel1 = row.get("rel1").toString();
                        final String rel2 = row.get("rel2").toString();
                        final String x = row.get("subjek").toString();
                        //pola1
                        //  final String AR = "Y-["+rel2+"]-> Z => X-["+rel1+"]->Y ";
                        //pola 2
                        final String AR = "Y-[" + rel2 + "]-> X => X-[" + rel1 + "]->Y ";
                        int a = Integer.valueOf(countHead);
                        int b = Integer.valueOf(x);
                        //listnode.add(new String[]{AR, rel1, rel2, x});

                        //int c = countHC(rel2);
                        double hc = (double) b / a;
                        if (hc >= (0.10000) && (!rel2.equals(rel1))) {
                            listnode.add(new String[]{AR, rel1, rel2, x, String.valueOf(hc), countHead});
                        } else {
                            continue;
                        }

                    }
                    tx.success();

                }
            }
        }


        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output, true), StandardCharsets.UTF_8));
             final CSVWriter csv = new CSVWriter(writer)) {
            csv.writeNext(new String[]{"Association Rule", "Relasi Head", "relasi2", "count X", "KC", "XHead"});
            listnode.forEach(csv::writeNext);
        }
    }

    public void ARPolaDua(File pathdb, File input, File output, String p) throws IOException, InterruptedException {

        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(pathdb);
        final List<String[]> listnode = new ArrayList<>();

        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(input), StandardCharsets.UTF_8), BUFFER_SIZE);
             final CSVReader csv = new CSVReader(reader)) {
            String[] nextLine;
            while ((nextLine = csv.readNext()) != null) {
                if (nextLine[0].startsWith("Relasi")) {
                    continue;
                }

                String relasiHead = nextLine[0];
                String countHead = nextLine[1];
                String Query = "";

                if (p.equals("a")) {
                    //pola umum 1
                    Query = "MATCH (y:owl_Thing)<-[r{rdf_type:'" + relasiHead + "'}]-(x:owl_Thing)-[r2]->(z:owl_Thing)-[r3]->(y:owl_Thing)  RETURN DISTINCT \"relationship\" AS element, \n" +
                            "r2.rdf_type As rel2,r3.rdf_type As rel3,count (distinct y.nn) AS objek,count (distinct x.nn) AS subjek order by subjek desc";
                } else if (p.equals("b")) {
                    //pola umum 2
                    Query = "MATCH (y:owl_Thing)<-[r{rdf_type:'" + relasiHead + "'}]-(x:owl_Thing)-[r2]->(z:owl_Thing)<-[r3]-(y:owl_Thing)  RETURN DISTINCT \"relationship\" AS element, \n" +
                            "r2.rdf_type As rel2,r3.rdf_type As rel3,count (distinct y.nn) AS objek,count (distinct x.nn) AS subjek order by subjek desc";
                } else if (p.equals("c")) {
                    //pola3
                    Query = "MATCH (y:owl_Thing)<-[r{rdf_type:'" + relasiHead + "'}]-(x:owl_Thing)<-[r2]-(z:owl_Thing)-[r3]->(y:owl_Thing)  RETURN DISTINCT \"relationship\" AS element, \n" +
                            "r2.rdf_type As rel2,r3.rdf_type As rel3,count (distinct y.nn) AS objek,count (distinct x.nn) AS subjek order by subjek desc";
                }


                Map<String, Object> params = new HashMap<String, Object>();
                try (Transaction tx = graphDb.beginTx();
                     Result result = graphDb.execute(Query, params)) {
                    while (result.hasNext()) {
                        final Map<String, Object> row = result.next();
                        final String rel3 = row.get("rel3").toString();
                        final String rel2 = row.get("rel2").toString();
                        final String x = row.get("subjek").toString();
                        final String y = row.get("objek").toString();
                        String AR = "";
                        if (p.equals("a")) {
                            //pola 1
                            AR = "X-[" + rel2 + "]->Z-[" + rel3 + "]->Y => X-[" + relasiHead + "]->Y ";
                        } else if (p.equals("b")) {
                            //pola 2
                            AR = "X-[" + rel2 + "]->Z<-[" + rel3 + "]-Y => X-[" + relasiHead + "]->Y ";
                            //pola 3
                        } else if (p.equals("c")) {
                            AR = "X<-[" + rel2 + "]-Z-[" + rel3 + "]->Y => X-[" + relasiHead + "]->Y ";
                        }

                        int aa = Integer.valueOf(countHead);
                        int bb = Integer.valueOf(x);
                        double cc = (double) bb / aa;

                        if (cc >= (0.10000)) {
                            listnode.add(new String[]{AR, relasiHead, rel2, rel3, x, String.valueOf(cc), String.valueOf(aa), p});
                        } else {
                            continue;
                        }

                    }
                    tx.success();
                }
            }
        }
        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output, true), StandardCharsets.UTF_8));
             final CSVWriter csv = new CSVWriter(writer)) {
            csv.writeNext(new String[]{"Association Rule", "Relasi Head", "Relasi1", "relasi2", "jumlah X", "CC", "X Head", "tipe"});
            listnode.forEach(csv::writeNext);
        }
    }
    public void ARPolaTiga(File pathdb, File input, File output,String p) throws IOException, InterruptedException {

        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(pathdb);
        final List<String[]> listnode = new ArrayList<>();

        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(input), StandardCharsets.UTF_8), BUFFER_SIZE);
             final CSVReader csv = new CSVReader(reader)) {
            String[] nextLine;

            while ((nextLine = csv.readNext()) != null) {
                if (nextLine[0].startsWith("Relasi")) {
                    continue;
                }

                String relasiHead = nextLine[0];
                String countHead = nextLine[1];
                String Query="";
                if(p.equals("a")) {
                    //pola  a
                    Query="MATCH (y:owl_Thing)<-[r{rdf_type:'"+relasiHead+"'}]-(x:owl_Thing)-[r1]->(z:owl_Thing)-[r2]->(w:owl_Thing)-[r3]->(y:owl_Thing)   RETURN DISTINCT \"relationship\" AS element, \n"+
                            "r1.rdf_type As rel1,r2.rdf_type As rel2,r3.rdf_type As rel3,count (distinct x.nn) AS subjek order by subjek desc";
                } else if(p.equals("b")) {
                    //pola  b
                    Query="MATCH (y:owl_Thing)<-[r{rdf_type:'"+relasiHead+"'}]-(x:owl_Thing)-[r1]->(z:owl_Thing)-[r2]->(w:owl_Thing)<-[r3]-(y:owl_Thing)   RETURN DISTINCT \"relationship\" AS element, \n"+
                            "r1.rdf_type As rel1,r2.rdf_type As rel2,r3.rdf_type As rel3,count (distinct x.nn) AS subjek order by subjek desc";
                } else if(p.equals("c")) {
                    //pola  c
                    Query="MATCH (y:owl_Thing)<-[r{rdf_type:'"+relasiHead+"'}]-(x:owl_Thing)<-[r1]-(z:owl_Thing)-[r2]->(w:owl_Thing)-[r3]->(y:owl_Thing)   RETURN DISTINCT \"relationship\" AS element, \n"+
                            "r1.rdf_type As rel1,r2.rdf_type As rel2,r3.rdf_type As rel3,count (distinct x.nn) AS subjek order by subjek desc";
                } else if(p.equals("d")) {
                    //pola  d
                    Query="MATCH (y:owl_Thing)<-[r{rdf_type:'"+relasiHead+"'}]-(x:owl_Thing)<-[r1]-(z:owl_Thing)-[r2]->(w:owl_Thing)<-[r3]-(y:owl_Thing)   RETURN DISTINCT \"relationship\" AS element, \n"+
                            "r1.rdf_type As rel1,r2.rdf_type As rel2,r3.rdf_type As rel3,count (distinct x.nn) AS subjek order by subjek desc";
                } else if(p.equals("e")) {
                    //pola  e
                    Query="MATCH (y:owl_Thing)<-[r{rdf_type:'"+relasiHead+"'}]-(x:owl_Thing)-[r1]->(z:owl_Thing)<-[r2]-(w:owl_Thing)-[r3]->(y:owl_Thing)   RETURN DISTINCT \"relationship\" AS element, \n"+
                            "r1.rdf_type As rel1,r2.rdf_type As rel2,r3.rdf_type As rel3,count (distinct x.nn) AS subjek order by subjek desc";
                } else if(p.equals("f")) {
                    //pola  f
                    Query = "MATCH (y:owl_Thing)<-[r{rdf_type:'" + relasiHead + "'}]-(x:owl_Thing)-[r1]->(z:owl_Thing)<-[r2]-(w:owl_Thing)<-[r3]-(y:owl_Thing)   RETURN DISTINCT \"relationship\" AS element, \n" +
                            "r1.rdf_type As rel1,r2.rdf_type As rel2,r3.rdf_type As rel3,count (distinct x.nn) AS subjek order by subjek desc";
                }
                Map<String, Object> params = new HashMap<String, Object>();
                try (Transaction tx = graphDb.beginTx();
                     Result result = graphDb.execute(Query,params)){
                    while (result.hasNext()) {
                        final Map<String, Object> row = result.next();
                        final String rel1 = row.get("rel1").toString();
                        final String rel3 = row.get("rel3").toString();
                        final String rel2 = row.get("rel2").toString();
                        final String x =  row.get("subjek").toString();
                        String AR ="";
                        if(p.equals("a")) {
                            //pola a
                            AR = "X-["+rel1+"]->Z-["+rel2+"]->W-["+rel3+"]->Y => X-["+relasiHead+"]->Y ";
                        } else if(p.equals("b")) {
                            //pola b
                            AR = "X-["+rel1+"]->Z-["+rel2+"]->W<-["+rel3+"]-Y => X-["+relasiHead+"]->Y ";
                        } else if(p.equals("c")) {
                            //pola c
                            AR = "X<-["+rel1+"]-Z-["+rel2+"]->W-["+rel3+"]->Y => X-["+relasiHead+"]->Y ";
                        } else if(p.equals("d")) {
                            //pola d
                            AR = "X<-["+rel1+"]-Z-["+rel2+"]->W<-["+rel3+"]-Y => X-["+relasiHead+"]->Y ";
                        } else if(p.equals("e")) {
                            //pola e
                            AR = "X-["+rel1+"]->Z<-["+rel2+"]-W-["+rel3+"]->Y => X-["+relasiHead+"]->Y ";
                        } else if(p.equals("f")) {
                            //pola f
                            AR = "X-["+rel1+"]->Z<-["+rel2+"]-W<-["+rel3+"]-Y => X-["+relasiHead+"]->Y ";
                        }
                        int aa = Integer.valueOf(countHead);
                        int bb= Integer.valueOf(x);

                        double cc = (double) bb/aa;

                        if (cc >=(0.1000)) {
                            listnode.add(new String[]{AR, relasiHead, rel1, rel2,rel3, x,String.valueOf(cc),String.valueOf(aa),p});
                        }else{
                            continue;
                        }
                    }
                    tx.success();
                }
            }
        }
        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output,true), StandardCharsets.UTF_8));
             final CSVWriter csv = new CSVWriter(writer)) {
            csv.writeNext(new String[]{"Association Rule","Relasi Head","Relasi1","relasi2","Relasi3", "jumlah X","CC","X Head","pola"});
            listnode.forEach(csv::writeNext);
        }
    }

    public void findConf(File pathdb, File input, File output, String p) throws IOException, InterruptedException {
        final List<String[]> listnode = new ArrayList<>();
        String AR = "", relhead = "", rel1 = "", rel2 = "", rel3 = "", KC = "", xRule = "", xHead = "", pola = "";
        int hasil = 0, hasilpca = 0;
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(input), StandardCharsets.UTF_8), BUFFER_SIZE);
             final CSVReader csv = new CSVReader(reader)) {
            String[] nextLine;
            String[] NodeX = null;
            //   log.info("Processing......");
            while ((nextLine = csv.readNext()) != null) {
                //nextLine=csv.readNext();
                if (nextLine[0].startsWith("Association Rule")) {
                    continue;
                }
                if (p.equals("A")) {
                    //untuk pola 2 relasi
                    AR = nextLine[0];
                    relhead = nextLine[1];
                    rel1 = nextLine[2];
                    rel2 = nextLine[3];
                    xRule = nextLine[4];
                    KC = nextLine[5];
                    xHead = nextLine[6];
                    pola = nextLine[7];

                    hasil = stdconf(pathdb, rel1, rel2, pola);
                    hasilpca = pcaconf(pathdb, rel1, rel2, relhead, pola);
                    //untuk mendapatkan detail node
                    NodeX = findNode(pathdb, rel1, rel2, relhead, pola);
                } else if (p.equals("B")) {
                    //untuk pola 3 relasi
                    AR = nextLine[0];
                    relhead = nextLine[1];
                    rel1 = nextLine[2];
                    rel2 = nextLine[3];
                    rel3 = nextLine[4];
                    xRule = nextLine[5];
                    KC = nextLine[6];
                    xHead = nextLine[7];
                    pola = nextLine[8];
                    hasil = stdconf2(pathdb, rel1, rel2, rel3, pola);
                    hasilpca = pcaconf2(pathdb, rel1, rel2, rel3, relhead, pola);
                    //untuk mendapatkan detail node
                    NodeX = findNode2(pathdb, rel1, rel2, rel3, relhead, pola);
                }
                int b = Integer.valueOf(xRule);
                double e = Double.valueOf(KC);
                double stdconf = (double) b / hasil;
                double pcaconf = (double) b / hasilpca;
                double liftconf = (double) b / (hasil * e);
                double liftpca = (double) b / (hasilpca * e);
                System.out.println("-----");
                listnode.add(new String[]{AR, relhead,xHead,xRule,KC, String.valueOf(stdconf), String.valueOf(pcaconf), String.valueOf(liftconf), String.valueOf(liftpca), (Arrays.asList(NodeX)).toString()});

            }
        }
        //   log.info("Writing {}  ...", listnode.size());
        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output, true), StandardCharsets.UTF_8));
             final CSVWriter csv = new CSVWriter(writer)) {
            csv.writeNext(new String[]{"Pola Graf", "Relasi Head","Jumlah Head", "jumlah X", "CC","Standar Confidence", "pca conf", "lift conf", "lift PCA", "Node"});
            listnode.forEach(csv::writeNext);
        }
        //  log.info("berhasil");
    }

    public int stdconf(File pathdb, String r1, String r2, String p) throws IOException, InterruptedException {
        int jumlahstd = 0;
        int jumlahA = 0;
        String Query = "";
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(pathdb);
        // standar confidence untuk 2 relasi
        if (p.equals("a")) {
            Query = "MATCH (x:owl_Thing)-[r{rdf_type:'" + r1 + "'}]->(z:owl_Thing)-[r2{rdf_type:'" + r2 + "'}]->(y:owl_Thing)RETURN count (distinct x) AS subjek";
        } else if (p.equals("b")) {
            Query = "MATCH (x:owl_Thing)-[r{rdf_type:'" + r1 + "'}]->(z:owl_Thing)<-[r2{rdf_type:'" + r2 + "'}]-(y:owl_Thing)RETURN count (distinct x) AS subjek";
        } else if (p.equals("c")) {
            Query = "MATCH (x:owl_Thing)<-[r{rdf_type:'" + r1 + "'}]-(z:owl_Thing)-[r2{rdf_type:'" + r2 + "'}]->(y:owl_Thing)RETURN DISTINCT count (distinct x) AS subjek";
        }

        //  log.info("{}",Query);
        Map<String, Object> params = new HashMap<String, Object>();
        try (Transaction tx = graphDb.beginTx();
             Result result = graphDb.execute(Query, params)) {
            while (result.hasNext()) {
                final Map<String, Object> row = result.next();
                //  final String jumlah3 =  row.get("jumlah3").toString();
                //   jumlahstd= Integer.valueOf(jumlah3);
                final String subjek = row.get("subjek").toString();
                jumlahA = Integer.valueOf(subjek);
                //       log.info("jumlah stdconf:  {}",subjek);
            }
            tx.success();
            //log.info("stdconf Sukses");
        }
        //   System.out.println( "mematikan koneksi neo4j untuk menghitung std conf ..." );
        // START SNIPPET: shutdownServer
        graphDb.shutdown();

        return jumlahA;
    }

    public int stdconf2(File pathdb, String r1, String r2, String r3, String p) throws IOException, InterruptedException {
        // int jumlahstd = 0;
        int jumlahA = 0;
        String Query = "";
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(pathdb);


        if (p.equals("a")) {
            Query = "MATCH (x:owl_Thing)-[r{rdf_type:'" + r1 + "'}]->(z:owl_Thing)-[r2{rdf_type:'" + r2 + "'}]->(w:owl_Thing)-[r3{rdf_type:'" + r3 + "'}]->(y:owl_Thing) RETURN count (distinct x.nn) AS subjek";
        } else if (p.equals("b")) {
            Query = "MATCH (x:owl_Thing)-[r{rdf_type:'" + r1 + "'}]->(z:owl_Thing)-[r2{rdf_type:'" + r2 + "'}]->(w:owl_Thing)<-[r3{rdf_type:'" + r3 + "'}]-(y:owl_Thing) RETURN count (distinct x.nn) AS subjek";
        } else if (p.equals("c")) {
            Query = "MATCH (x:owl_Thing)<-[r{rdf_type:'" + r1 + "'}]-(z:owl_Thing)-[r2{rdf_type:'" + r2 + "'}]->(w:owl_Thing)-[r3{rdf_type:'" + r3 + "'}]->(y:owl_Thing) RETURN count (distinct x.nn) AS subjek";
        } else if (p.equals("d")) {
            Query = "MATCH (x:owl_Thing)<-[r{rdf_type:'" + r1 + "'}]-(z:owl_Thing)-[r2{rdf_type:'" + r2 + "'}]->(w:owl_Thing)<-[r3{rdf_type:'" + r3 + "'}]-(y:owl_Thing) RETURN count (distinct x.nn) AS subjek";
        } else if (p.equals("e")) {
            Query = "MATCH (x:owl_Thing)-[r{rdf_type:'" + r1 + "'}]->(z:owl_Thing)<-[r2{rdf_type:'" + r2 + "'}]-(w:owl_Thing)-[r3{rdf_type:'" + r3 + "'}]->(y:owl_Thing) RETURN count (distinct x.nn) AS subjek";
        } else if (p.equals("f")) {
            Query = "MATCH (x:owl_Thing)-[r{rdf_type:'" + r1 + "'}]->(z:owl_Thing)<-[r2{rdf_type:'" + r2 + "'}]-(w:owl_Thing)<-[r3{rdf_type:'" + r3 + "'}]-(y:owl_Thing) RETURN count (distinct x.nn) AS subjek";
        }
        //  log.info("{}",Query);
        Map<String, Object> params = new HashMap<String, Object>();
        try (Transaction tx = graphDb.beginTx();
             Result result = graphDb.execute(Query, params)) {
            while (result.hasNext()) {
                final Map<String, Object> row = result.next();
                //  final String jumlah3 =  row.get("jumlah3").toString();
                //   jumlahstd= Integer.valueOf(jumlah3);
                final String subjek = row.get("subjek").toString();
                jumlahA = Integer.valueOf(subjek);
                //      log.info("jumlah stdconf:  {}",subjek);
            }
            tx.success();
            //log.info("stdconf Sukses");
        }
        //   System.out.println( "mematikan koneksi neo4j untuk menghitung std conf ..." );
        // START SNIPPET: shutdownServer
        graphDb.shutdown();

        return jumlahA;
    }

    public int pcaconf(File pathdb, String r1, String r2, String rhead, String p) throws IOException, InterruptedException {
        int nodepca = 0;
        String Query = "";
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(pathdb);
        //pca conf 2 relasi
        if (p.equals("a")) {
            Query = "MATCH (a:owl_Thing)<-[r3{rdf_type:'" + rhead + "'}]-(x:owl_Thing)-[r{rdf_type:'" + r1 + "'}]->(z:owl_Thing)-[r2{rdf_type:'" + r2 + "'}]->(y:owl_Thing) RETURN DISTINCT \"relationship\" AS element, \n" +
                    "count (distinct x.nn) AS subjek";
        } else if (p.equals("b")) {
            Query = "MATCH (a:owl_Thing)<-[r3{rdf_type:'" + rhead + "'}]-(x:owl_Thing)-[r{rdf_type:'" + r1 + "'}]->(z:owl_Thing)<-[r2{rdf_type:'" + r2 + "'}]-(y:owl_Thing)RETURN DISTINCT \"relationship\" AS element, \n" +
                    "count (distinct x.nn) AS subjek";
        } else if (p.equals("c")) {
            Query = "MATCH (a:owl_Thing)<-[r3{rdf_type:'" + rhead + "'}]-(x:owl_Thing)<-[r{rdf_type:'" + r1 + "'}]-(z:owl_Thing)-[r2{rdf_type:'" + r2 + "'}]->(y:owl_Thing)RETURN DISTINCT \"relationship\" AS element, \n" +
                    "count (distinct x.nn) AS subjek";
        }

        // log.info("{}",Query);
        Map<String, Object> params = new HashMap<String, Object>();
        try (Transaction tx = graphDb.beginTx();
             Result result = graphDb.execute(Query, params)) {
            while (result.hasNext()) {
                final Map<String, Object> row = result.next();
                //    final String jumlah3 =  row.get("jumlah3").toString();
                //    jumlahpca= Integer.valueOf(jumlah3);
                final String subjek = row.get("subjek").toString();
                nodepca = Integer.valueOf(subjek);
                //         log.info("jumlah node pca: {}",nodepca);
            }
            tx.success();
            //    log.info("PCA Sukses");
        }
        // System.out.println( "mematikan koneksi neo4j untuk menghitung pca conf ..." );
        // START SNIPPET: shutdownServer
        graphDb.shutdown();

        return nodepca;
    }

    public int pcaconf2(File pathdb, String r1, String r2, String r3, String rhead, String p) throws IOException, InterruptedException {

        int nodepca = 0;
        String Query = "";
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(pathdb);
        //pca conf 3 relasi
        if (p.equals("a")) {
            Query = "MATCH (a:owl_Thing)<-[r1{rdf_type:'" + rhead + "'}]-(x:owl_Thing)-[r{rdf_type:'" + r1 + "'}]->(z:owl_Thing)-[r2{rdf_type:'" + r2 + "'}]->(w:owl_Thing)-[r3{rdf_type:'" + r3 + "'}]->(y:owl_Thing) RETURN count (distinct x.nn) AS subjek";
        } else if (p.equals("b")) {
            Query = "MATCH (a:owl_Thing)<-[r1{rdf_type:'" + rhead + "'}]-(x:owl_Thing)-[r{rdf_type:'" + r1 + "'}]->(z:owl_Thing)-[r2{rdf_type:'" + r2 + "'}]->(w:owl_Thing)<-[r3{rdf_type:'" + r3 + "'}]-(y:owl_Thing) RETURN count (distinct x.nn) AS subjek";
        } else if (p.equals("c")) {
            Query = "MATCH (a:owl_Thing)<-[r1{rdf_type:'" + rhead + "'}]-(x:owl_Thing)<-[r{rdf_type:'" + r1 + "'}]-(z:owl_Thing)-[r2{rdf_type:'" + r2 + "'}]->(w:owl_Thing)-[r3{rdf_type:'" + r3 + "'}]->(y:owl_Thing) RETURN count (distinct x.nn) AS subjek";
        } else if (p.equals("d")) {
            Query = "MATCH (a:owl_Thing)<-[r1{rdf_type:'" + rhead + "'}]-(x:owl_Thing)<-[r{rdf_type:'" + r1 + "'}]-(z:owl_Thing)-[r2{rdf_type:'" + r2 + "'}]->(w:owl_Thing)<-[r3{rdf_type:'" + r3 + "'}]-(y:owl_Thing) RETURN count (distinct x.nn) AS subjek";
        } else if (p.equals("e")) {
            Query = "MATCH (a:owl_Thing)<-[r1{rdf_type:'" + rhead + "'}]-(x:owl_Thing)-[r{rdf_type:'" + r1 + "'}]->(z:owl_Thing)<-[r2{rdf_type:'" + r2 + "'}]-(w:owl_Thing)-[r3{rdf_type:'" + r3 + "'}]->(y:owl_Thing) RETURN count (distinct x.nn) AS subjek";
        } else if (p.equals("f")) {
            Query = "MATCH (a:owl_Thing)<-[r1{rdf_type:'" + rhead + "'}]-(x:owl_Thing)-[r{rdf_type:'" + r1 + "'}]->(z:owl_Thing)<-[r2{rdf_type:'" + r2 + "'}]-(w:owl_Thing)<-[r3{rdf_type:'" + r3 + "'}]-(y:owl_Thing) RETURN count (distinct x.nn) AS subjek";
        }
        //   log.info("{}",Query);
        Map<String, Object> params = new HashMap<String, Object>();
        try (Transaction tx = graphDb.beginTx();
             Result result = graphDb.execute(Query, params)) {
            while (result.hasNext()) {
                final Map<String, Object> row = result.next();
                //    final String jumlah3 =  row.get("jumlah3").toString();
                //    jumlahpca= Integer.valueOf(jumlah3);
                final String subjek = row.get("subjek").toString();
                nodepca = Integer.valueOf(subjek);
            }
            tx.success();
        }
        // System.out.println( "mematikan koneksi neo4j untuk menghitung pca conf ..." );
        // START SNIPPET: shutdownServer
        graphDb.shutdown();

        return nodepca;
    }


    public String[] findNode(File pathdb, String r1, String r2, String rhead, String p) throws IOException, InterruptedException {

        String Query = "";
        ArrayList<String> AddX = new ArrayList<>();
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(pathdb);
        //pca conf 2 relasi
        if (p.equals("a")) {
            Query = "MATCH (y:owl_Thing)<-[r3{rdf_type:'" + rhead + "'}]-(x:owl_Thing)-[r{rdf_type:'" + r1 + "'}]->(z:owl_Thing)-[r2{rdf_type:'" + r2 + "'}]->(y:owl_Thing) RETURN DISTINCT x";
        } else if (p.equals("b")) {
            Query = "MATCH (y:owl_Thing)<-[r3{rdf_type:'" + rhead + "'}]-(x:owl_Thing)-[r{rdf_type:'" + r1 + "'}]->(z:owl_Thing)<-[r2{rdf_type:'" + r2 + "'}]-(y:owl_Thing)RETURN DISTINCT x";
        } else if (p.equals("c")) {
            Query = "MATCH (y:owl_Thing)<-[r3{rdf_type:'" + rhead + "'}]-(x:owl_Thing)<-[r{rdf_type:'" + r1 + "'}]-(z:owl_Thing)-[r2{rdf_type:'" + r2 + "'}]->(y:owl_Thing)RETURN DISTINCT x";
        }

        // log.info("{}",Query);
        Map<String, Object> params = new HashMap<String, Object>();
        try (Transaction tx = graphDb.beginTx();
             Result result = graphDb.execute(Query, params)) {
            while (result.hasNext()) {
                final Map<String, Object> row = result.next();
                final Node x = (Node) row.get("x");
                final String NodeX = x.getProperty("nn").toString().replace(",", "");
                AddX.add((String) NodeX);
            }
            tx.success();
        }
        graphDb.shutdown();
        return AddX.toArray(new String[AddX.size()]);
    }

    public String[] findNode2(File pathdb, String r1, String r2, String r3, String rhead, String p) throws IOException, InterruptedException {

        String Query = "";
        ArrayList<String> AddX = new ArrayList<>();
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(pathdb);
        //3 relasi
        if (p.equals("a")) {
            Query = "MATCH (y:owl_Thing)<-[r1{rdf_type:'" + rhead + "'}]-(x:owl_Thing)-[r{rdf_type:'" + r1 + "'}]->(z:owl_Thing)-[r2{rdf_type:'" + r2 + "'}]->(w:owl_Thing)-[r3{rdf_type:'" + r3 + "'}]->(y:owl_Thing) RETURN distinct x";
        } else if (p.equals("b")) {
            Query = "MATCH (y:owl_Thing)<-[r1{rdf_type:'" + rhead + "'}]-(x:owl_Thing)-[r{rdf_type:'" + r1 + "'}]->(z:owl_Thing)-[r2{rdf_type:'" + r2 + "'}]->(w:owl_Thing)<-[r3{rdf_type:'" + r3 + "'}]-(y:owl_Thing) RETURN distinct x";
        } else if (p.equals("c")) {
            Query = "MATCH (y:owl_Thing)<-[r1{rdf_type:'" + rhead + "'}]-(x:owl_Thing)<-[r{rdf_type:'" + r1 + "'}]-(z:owl_Thing)-[r2{rdf_type:'" + r2 + "'}]->(w:owl_Thing)-[r3{rdf_type:'" + r3 + "'}]->(y:owl_Thing) RETURN distinct x";
        } else if (p.equals("d")) {
            Query = "MATCH (y:owl_Thing)<-[r1{rdf_type:'" + rhead + "'}]-(x:owl_Thing)<-[r{rdf_type:'" + r1 + "'}]-(z:owl_Thing)-[r2{rdf_type:'" + r2 + "'}]->(w:owl_Thing)<-[r3{rdf_type:'" + r3 + "'}]-(y:owl_Thing) RETURN distinct x";
        } else if (p.equals("e")) {
            Query = "MATCH (y:owl_Thing)<-[r1{rdf_type:'" + rhead + "'}]-(x:owl_Thing)-[r{rdf_type:'" + r1 + "'}]->(z:owl_Thing)<-[r2{rdf_type:'" + r2 + "'}]-(w:owl_Thing)-[r3{rdf_type:'" + r3 + "'}]->(y:owl_Thing) RETURN distinct x";
        } else if (p.equals("f")) {
            Query = "MATCH (y:owl_Thing)<-[r1{rdf_type:'" + rhead + "'}]-(x:owl_Thing)-[r{rdf_type:'" + r1 + "'}]->(z:owl_Thing)<-[r2{rdf_type:'" + r2 + "'}]-(w:owl_Thing)<-[r3{rdf_type:'" + r3 + "'}]-(y:owl_Thing) RETURN distinct x";
        }

        // log.info("{}",Query);
        Map<String, Object> params = new HashMap<String, Object>();
        try (Transaction tx = graphDb.beginTx();
             Result result = graphDb.execute(Query, params)) {
            while (result.hasNext()) {
                final Map<String, Object> row = result.next();
                final Node x = (Node) row.get("x");
                final String NodeX = x.getProperty("nn").toString().replace(",", "");
                AddX.add((String) NodeX);
            }
            tx.success();
        }
        graphDb.shutdown();
        return AddX.toArray(new String[AddX.size()]);
    }
    public void optimasi2(File input, File output)throws IOException, InterruptedException{
        HashSet<String> unionSet = new HashSet<String>();
        final List<String[]> listnode = new ArrayList<>();
        final List<String[]> listsimpan = new ArrayList<>();

        Set<String> listnode2 = new HashSet<>();
        double a=0,b=0,diff=0,diffx=0;
        double liftpca=0,opt =0;
        String d="",c="";
        String[] NodeX1,NodeX2;
        int count=0,m=0;
        String head="";
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(input), StandardCharsets.UTF_8), BUFFER_SIZE);
             final CSVReader csv = new CSVReader(reader)) {
            String[] nextLine;
            while ((nextLine = csv.readNext()) != null) {
                //nextLine=csv.readNext();
                if (nextLine[0].startsWith("Aturan Asosiasi")) {
                    continue;
                }
                prop1[count] = nextLine[7];
                head = nextLine[1];
                double liftpcaconf = Double.valueOf(nextLine[6]);
                liftpca = liftpca+liftpcaconf;
                count++;
            }
        }
        int n = count;
        liftpca = liftpca/count;

        for (int i = 0; i < n-1; i++) {
            for (int j = i + 1; j < n; j++) {
                c=prop1[i];
                d=prop1[j];
                NodeX1 = c.split(",");
                NodeX2 = d.split(",");
                // listnode2.add(NodeX1);
                int k, l;
                for (k = 0; k < NodeX1.length; k++) {
                    listnode2.add(NodeX1[k]);
                    //     log.info("querynode1 {} = {} ", k, NodeX1[k]);
                }
                for (l = 0; l < NodeX2.length; l++) {
                    //  log.info("querynode2 {} = {}", l, NodeX2[l]);
                    listnode2.add(NodeX2[l]);
                }
                System.out.println("The union of both the arrays is");
                int t = 0;
                ArrayList<String>union= new ArrayList<>();
                for (String I : listnode2) {
                    //System.out.print(I + " ");
                    union.add(I);
                    t++;
                }
                System.out.println("Jumlah union=" +t);
                listnode2.clear();
                System.out.println("The intersection of both the arrays is");
                for (k = 0; k < NodeX1.length; k++) {
                    listnode2.add(NodeX1[k]);
                }
                int co = 0;
                ArrayList<String>intersecion= new ArrayList<>();
                for (l = 0; l < NodeX2.length; l++) {
                    if (listnode2.contains(NodeX2[l])) {
                        //  System.out.print(NodeX2[l] + " ");
                        intersecion.add(NodeX2[l]);
                        co = co + 1;
                    }

                }

                System.out.println("jumlahn intersec= "+co+"yaitu :"+intersecion);

                diff=1-((double)co/t);
                String gabung = "R"+String.valueOf(i+1)+ " R" + String.valueOf(j+1);
                listnode.add(new String[]{gabung,String.valueOf(diff),String.valueOf(t),String.valueOf(co)});
                diffx = diffx+diff;
            }
        }
        diffx = diffx/listnode.size();
  //      log.info("rata rata dif: {}",diffx);
        // diff = (1- (float)(b/a));
        opt = (0.5 *liftpca)+diffx;
   //     log.info("rata2 diff={}  ",diffx);
        listsimpan.add(new String[]{head,String.valueOf(liftpca),String.valueOf(diffx),String.valueOf(opt)});
        // log.info("Writing {}  ...", listsimpan.size());
     //   log.info("Writing {}  ...", listnode.size());
        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output), StandardCharsets.UTF_8));
             final CSVWriter csv = new CSVWriter(writer)) {
            csv.writeNext(new String[]{"rule","diff","Jumlah Union","Jumlah intersection"});
            listnode.forEach(csv::writeNext);
            csv.writeNext(new String[]{"Head","Lift PCA","diff","Nilai Optimasi"});
            listsimpan.forEach(csv::writeNext);
            //  csv.writeAll((List<String[]>) coba);
        }
     //   log.info("berhasil");

    }
}
