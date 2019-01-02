package jonathan.mayer.calciteh2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.util.LinkedHashSet;

import org.h2.tools.SimpleResultSet;

public class CalciteSQLGenerator {

	public CalciteSQLGenerator() {
	}

	public static ResultSet getFunctionInputs(Connection conn, String stname, String vid)
			throws SQLException, ParseException {

		final SimpleResultSet output = prepareResultSet();

		String sql = String.format(
				"SELECT * from VW_STVARIANTS WHERE STNAME='%s' AND VARIANTNAME='%s' ORDER BY INPUTID", stname, vid);

		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet result = pstmt.executeQuery();
		while (result.next()) {

			String vname = result.getString("NAME");
			output.addRow(stname, vid, vname);

		}

		return output;
	}

	public static ResultSet getFunctionVariants(Connection conn, String stname) throws SQLException, ParseException {
		final SimpleResultSet output = prepareResultSet2();

		String sql = String.format("SELECT DISTINCT VARIANTNAME from VW_STVARIANTS WHERE STNAME='%s' ", stname);

		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet result = pstmt.executeQuery();
		while (result.next()) {

			String vid = result.getString("VARIANTNAME");
			output.addRow(stname, vid);

		}

		return output;
	}

	public static String getFunctionInputs2(Connection conn, String stname, String vid)
			throws SQLException, ParseException {
		StringBuilder sb = new StringBuilder();

		String sql = String.format(
				"SELECT * from VW_STVARIANTS WHERE STNAME='%s' AND VARIANTNAME='%s' ORDER BY INPUTID", stname, vid);

		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet result = pstmt.executeQuery();
		int inputid = 1;
		while (result.next()) {

			String vname = result.getString("NAME");
			sb.append(String.format("%s %s,", vname, "input" + inputid));
			// output.addRow(stname,vid,vname);

			inputid += 1;

		}
		String s = sb.toString().substring(0, sb.toString().length() - 1);

		return stname + "(" + s + ")";
	}

	private static SimpleResultSet prepareResultSet2() {
		SimpleResultSet output = new SimpleResultSet();

		output.addColumn("stname", Types.VARCHAR, 0, 0);
		output.addColumn("vid", Types.VARCHAR, 0, 0);

		return output;
	}

	private static SimpleResultSet prepareResultSet() {
		SimpleResultSet output = new SimpleResultSet();

		output.addColumn("stname", Types.VARCHAR, 0, 0);
		output.addColumn("vid", Types.VARCHAR, 0, 0);
		output.addColumn("name", Types.VARCHAR, 0, 0);
		return output;
	}
}
