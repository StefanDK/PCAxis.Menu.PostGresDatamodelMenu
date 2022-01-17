using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Diagnostics;
using Npgsql;
using NpgsqlTypes;
using PCAxis.Menu.Implementations;

namespace PCAxis.Menu.Implementations
{
	/// <summary>
	/// DatamodelMenu implementation for usage with PostGres.
	/// </summary>
	public class PostGresDatamodelMenu : DatamodelMenu
	{
		private static readonly log4net.ILog log = log4net.LogManager.GetLogger(typeof(PostGresDatamodelMenu));

		private string connectionString;

		/// <summary>
		/// DatamodelMenu implementation for usage with PostGres.
		/// </summary>
		/// <param name="connectionString">The connection string for the database</param>
		/// <param name="language">Language code for retrived data</param>
		/// <param name="initializationFunction">Lambda for initialization of the menu instance</param>
		public PostGresDatamodelMenu(string connectionString, string language, DatamodelMenuInitialization initializationFunction)
			: base(
				'@',
				language,
				m =>
				{
					((PostGresDatamodelMenu)m).connectionString = connectionString;

					m.AddSqlHints(SqlHint.UseRecursiveCTE);
					m.AddSqlHints(SqlHint.UseExtraSelect);
					m.AlterSQL = (sql) => sql.Replace("select * from (with base", "with recursive base");
					if (initializationFunction != null)
						initializationFunction(m);
				}
			)
		{ }

		/// <summary>
		/// </summary>
		public override DataTable GetDataTableBySelection(string menu, string selection, int numberOfLevels, string sql, DatabaseParameterCollection parameters)
		{
			log.DebugFormat("Getting menu for menu, selection, numberOfLevels: {0}, {1}, {2}", menu, selection, numberOfLevels);
			log.DebugFormat("SQL: {0}", sql);

			DataTable dataTable = new DataTable();

			using (NpgsqlConnection connection = new NpgsqlConnection(connectionString))
			{
				connection.Open();

				NpgsqlCommand command = new NpgsqlCommand(sql, connection)/* { BindByName = true }*/;

				command.Parameters.AddWithValue("levels", NpgsqlDbType.Integer, parameters["levels"].Size, numberOfLevels);
				command.Parameters.AddWithValue("menu", NpgsqlDbType.Varchar, parameters["menu"].Size, menu);
				command.Parameters.AddWithValue("selection", NpgsqlDbType.Varchar, parameters["selection"].Size, selection);
				try
				{
					new NpgsqlDataAdapter(command).Fill(dataTable);
				}
				catch (NpgsqlException e)
				{
					log.Error(e);
				}
			}

			return dataTable;
		}
	}
}
