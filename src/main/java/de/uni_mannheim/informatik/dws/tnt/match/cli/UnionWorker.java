package de.uni_mannheim.informatik.dws.tnt.match.cli;


import au.com.bytecode.opencsv.CSVWriter;
import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.stitching.UnionTables;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class UnionWorker {

    private String domain;
    private Connection con;
    private int batchsize = 5000;

    public UnionWorker(String domain, Connection con) {
        this.domain = domain;
        this.con = con;
    }

    public void execute(int unionid) {
        //------------ Union Tables ---------------------
        System.err.println("Loading Web Tables");
        try {
            WebTables web = WebTables.loadVerticaWebTables(
                    con, domain, true, false, false, false);
            UnionTables union = new UnionTables();
            Map<String, Integer> contextAttributes = null;

            System.err.println("Creating Context Attributes");
            contextAttributes = union.generateContextAttributes(web.getTables().values(), true, false);

            System.err.println("Creating Union Tables");
            Collection<Table> unionTables = union.create(new ArrayList<>(web.getTables().values()), contextAttributes);

            System.err.println("Writing Union Tables");
            VerticaCopyStream unionStream = new VerticaCopyStream(
                    (VerticaConnection) con, "COPY union_tables"
                    + " (tableid, unionid) "
                    + " FROM STDIN "
                    + " DELIMITER ',' ENCLOSED BY '\"' DIRECT"
                    + " REJECTED DATA './logs/rejectedStitchedTables.txt'");
            unionStream.start();

            int processed = 0;
            unionid++;
            for (Table t : unionTables) {
                ArrayList<TableColumn> columns = (ArrayList<TableColumn>) t.getColumns();
                TableColumn column = columns.get(columns.size() - 1);
                for (String str : column.getProvenance()) {
                    int tableid = Integer.valueOf(str.substring(str.lastIndexOf('#') + 1, str.lastIndexOf('~')));

                    StringWriter stringWriter = new StringWriter();
                    CSVWriter unionWriter = new CSVWriter(stringWriter, ',', '"', '\\');
                    unionWriter.writeNext(new String[]{Integer.toString(tableid), Integer.toString(unionid)});
                    unionWriter.flush();
                    unionWriter.close();
                    String unionString = stringWriter.toString();
                    unionStream.addStream(IOUtils.toInputStream(unionString));

                    processed++;
                }
                if ((processed % batchsize) == 0) {
                    unionStream.execute();
                }
                unionid++;
            }

            unionStream.execute();
            unionStream.finish();
            CreateUnionTablesVertica.setUnionId(unionid);
            System.out.println(processed + " tables written!");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
