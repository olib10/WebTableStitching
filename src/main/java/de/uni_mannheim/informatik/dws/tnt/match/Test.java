package de.uni_mannheim.informatik.dws.tnt.match;

import com.google.gson.Gson;
import de.uni_mannheim.informatik.dws.tnt.match.data.JsonContentSchema;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.JsonTableParser;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Arrays;

public class Test {

    private static String temp = "112990315|dwtc-385.json.gz#112990315|1|http://www.unece.org/es/env/eia/guidance/welcome.html|unece.org|1 1st 2nd 8 about activities activity agreements amendment application applying areas assessment assessments authorities bilateral bodies building can capacity change climate commission committee compliance contact contents convention cooperation developed development document eastern economic eia eng environment environmental espoo europe events executive format fre ger green guidance health home ii 2 impact impacts implementation information initiative introduction issues lead legislation map mb meeting meetings multilateral national nations notification other outputs overview pages participation parties points policy practical procedures protocol public publications ratification review rus sea sessions specific subregional summary text through title topics transboundary transport under unece united us use work|{\"schema\":{\"columnNames\":[\"Document Title\",\"ENG+FRE+RUS\"],\"columnTypes\":[\"String\",\"String\"]},\"numRows\":1,\"numCols\":2,\"tuples\":[{\"cm\":{\"columnNames\":[\"Document Title\",\"ENG+FRE+RUS\"],\"columnTypes\":[\"String\",\"String\"]},\"cells\":[{\"type\":\"String\",\"value\":\"ECE/MP.EIA/8 Guidance on the Practical Application of the Espoo Convention Directives concernant l\\u0027application concrète de la Convention d\\u0027Espoo Руководство по практиприменению Конвенции Эспоо (No. 8 in the environment series)\"},{\"type\":\"String\",\"value\":\"ENG+FRE+RUS\"}],\"tid\":0}],\"openrank\":0,\"confidence\":0.0,\"source\":-1,\"tableId\":0}";

    public static void main(String[] args) throws URISyntaxException, UnsupportedEncodingException {
        WebTables web = new WebTables();
        JsonTableParser jsonParser = new JsonTableParser();
        jsonParser.setInferSchema(true);
        jsonParser.setConvertValues(true);
        Gson gson = new Gson();
        Table table = web.tableFromDump(temp.split("\\|"), jsonParser, gson);
        System.out.println(TableSchemaStatistics.generateSchemaString(table));
        System.out.println(table);
    }
}
