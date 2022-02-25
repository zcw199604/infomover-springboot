package com.info.infomover.crawler;

import com.google.common.collect.Lists;
import com.info.baymax.common.utils.ICollections;
import com.info.infomover.crawler.entity.*;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import schemacrawler.inclusionrule.ExcludeAll;
import schemacrawler.inclusionrule.IncludeAll;
import schemacrawler.inclusionrule.InclusionRule;
import schemacrawler.inclusionrule.RegularExpressionInclusionRule;
import schemacrawler.schema.*;
import schemacrawler.schemacrawler.*;
import schemacrawler.tools.utility.SchemaCrawlerUtility;

import java.sql.Connection;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class CrawlerUtils {

	public static CrawlerCatalog fetchCatalog(Connection conn, String schemaInclusionRule)
		throws SchemaCrawlerException {
		return covertCatalog(crawlCatalog(conn, schemaInclusionRule));
	}

	public static Catalog crawlCatalogOnlySchemas(Connection conn, String schemaInclusionRule)
		throws SchemaCrawlerException {
		return crawlCatalog(conn, new RegularExpressionInclusionRule(schemaInclusionRule), new ExcludeAll(),
			new ExcludeAll(), new ExcludeAll(), new ExcludeAll(), new ExcludeAll());
	}

	public static Catalog crawlCatalogOnlySchemasAndTables(Connection conn, String schemaInclusionRule)
		throws SchemaCrawlerException {
		return crawlCatalog(conn, new RegularExpressionInclusionRule(schemaInclusionRule), new IncludeAll(),
			new ExcludeAll(), new ExcludeAll(), new ExcludeAll(), new ExcludeAll());
	}

	public static Catalog crawlCatalog(Connection conn, String schemaInclusionRule, String tableInclusionRule)
		throws SchemaCrawlerException {
		return crawlCatalog(conn, new RegularExpressionInclusionRule(schemaInclusionRule),
			new RegularExpressionInclusionRule(tableInclusionRule), null, null, null, null);
	}

	public static Catalog crawlCatalog(Connection conn, String schemaInclusionRule) throws SchemaCrawlerException {
		return crawlCatalog(conn, new RegularExpressionInclusionRule(schemaInclusionRule), null, null, null, null,
			null);
	}

	public static Catalog crawlCatalog(Connection conn, String schemaInclusionRule, String tableInclusionRule,
									   String columnInclusionRule, String routineInclusionRule, String routineParameterInclusionRule,
									   String sequenceInclusionRule) throws SchemaCrawlerException {
		return crawlCatalog(conn, new RegularExpressionInclusionRule(schemaInclusionRule),
			new RegularExpressionInclusionRule(tableInclusionRule),
			new RegularExpressionInclusionRule(columnInclusionRule),
			new RegularExpressionInclusionRule(routineInclusionRule),
			new RegularExpressionInclusionRule(routineParameterInclusionRule),
			new RegularExpressionInclusionRule(sequenceInclusionRule));
	}

	public static Catalog crawlCatalog(Connection conn, InclusionRule schemaInclusionRule,
									   InclusionRule tableInclusionRule, InclusionRule columnInclusionRule, InclusionRule routineInclusionRule,
									   InclusionRule routineParameterInclusionRule, InclusionRule sequenceInclusionRule)
		throws SchemaCrawlerException {
		LimitOptionsBuilder limitOptionsBuilder = LimitOptionsBuilder.builder();//

		if (schemaInclusionRule != null) {
			limitOptionsBuilder.includeSchemas(schemaInclusionRule);
		}
		if (tableInclusionRule != null) {
			limitOptionsBuilder.includeTables(tableInclusionRule);
		}
		if (columnInclusionRule != null) {
			limitOptionsBuilder.includeColumns(columnInclusionRule);
		}
		if (routineInclusionRule != null) {
			limitOptionsBuilder.includeRoutines(routineInclusionRule);
		}
		if (routineParameterInclusionRule != null) {
			limitOptionsBuilder.includeRoutineParameters(routineParameterInclusionRule);
		}
		if (sequenceInclusionRule != null) {
			limitOptionsBuilder.includeSequences(sequenceInclusionRule);
		}

		LoadOptionsBuilder loadOptionsBuilder = LoadOptionsBuilder.builder()
			.withSchemaInfoLevel(SchemaInfoLevelBuilder.maximum());
		return SchemaCrawlerUtility.getCatalog(conn, SchemaCrawlerOptionsBuilder//
			.newSchemaCrawlerOptions().withLimitOptions(limitOptionsBuilder.toOptions())
			.withLoadOptions(loadOptionsBuilder.toOptions()));
	}

	public static CrawlerCatalog covertCatalog(Catalog catalog) {
		CrawlerCatalog crawlerCatalog = copyProperties(catalog, new CrawlerCatalog());
		List<CrawlerSchema> schemas = covertSchemas(catalog, catalog.getSchemas());
		if (ICollections.hasElements(schemas)) {
			crawlerCatalog.setSchemas(schemas);
		}
		return crawlerCatalog;
	}

	public static List<CrawlerSchema> covertSchemas(Catalog catalog, Collection<Schema> schemas) {
		if (ICollections.hasElements(schemas)) {
			return schemas.stream().map(t -> covertSchema(catalog, t)).collect(Collectors.toList());
		}
		return Lists.newArrayList();
	}

	public static CrawlerSchema covertSchema(Catalog catalog, Schema schema) {
		CrawlerSchema crawlerSchema = copyProperties(schema, new CrawlerSchema());
		crawlerSchema.setCatalogName(schema.getCatalogName());
		List<CrawlerTable> tables = covertTables(catalog.getTables(schema));
		crawlerSchema.setTables(tables);
		crawlerSchema.setTableCount(ICollections.hasElements(tables) ? tables.size() : 0);
		return crawlerSchema;
	}

	public static List<CrawlerTable> covertTables(Collection<Table> tables) {
		if (ICollections.hasElements(tables)) {
			return tables.stream().sorted(Comparator.comparing(Table::getName)).map(t -> covertTable(t))
				.collect(Collectors.toList());
		}
		return Lists.newArrayList();
	}

	public static CrawlerTable covertTable(Table table) {
		CrawlerTable crawlerTable = copyProperties(table, new CrawlerTable());
		crawlerTable.setDefinition(table.getDefinition());
		crawlerTable.setTableType(table.getTableType().getTableType());
		crawlerTable.setPrimaryKey(covertPrimaryKey(table.getPrimaryKey()));
		Collection<ForeignKey> foreignKeys = table.getForeignKeys();
		crawlerTable.setForeignKeys(covertForeignKeys(foreignKeys));
		crawlerTable.setColumns(covertColumns(table.getColumns()));
		return crawlerTable;
	}

	public static CrawlerPrimaryKey covertPrimaryKey(PrimaryKey primaryKey) {
		if (primaryKey != null) {
			CrawlerPrimaryKey crawlerPrimaryKey = new CrawlerPrimaryKey();
			crawlerPrimaryKey.setName(primaryKey.getName());
			crawlerPrimaryKey.setFullName(primaryKey.getFullName());
			crawlerPrimaryKey.setRemarks(primaryKey.getRemarks());
			crawlerPrimaryKey.setShortName(primaryKey.getShortName());

			List<TableConstraintColumn> constrainedColumns = primaryKey.getConstrainedColumns();
			if (ICollections.hasElements(constrainedColumns)) {
				crawlerPrimaryKey.setConstraintColumns(
					constrainedColumns.stream().map(t -> t.getName()).collect(Collectors.toList()));
			}
			return crawlerPrimaryKey;
		}
		return null;
	}

	public static List<CrawlerForeignKey> covertForeignKeys(Collection<ForeignKey> foreignKeys) {
		if (ICollections.hasElements(foreignKeys)) {
			return foreignKeys.stream().map(t -> covertForeignKey(t)).collect(Collectors.toList());
		}
		return Lists.newArrayList();
	}

	public static CrawlerForeignKey covertForeignKey(ForeignKey foreignKey) {
		if (foreignKey != null) {
			CrawlerForeignKey crawlerForeignKey = copyProperties(foreignKey, new CrawlerForeignKey());
			crawlerForeignKey.setShortName(foreignKey.getShortName());
			crawlerForeignKey.setDefinition(foreignKey.getDefinition());
			crawlerForeignKey.setSpecificName(foreignKey.getSpecificName());

			Table primaryKeyTable = foreignKey.getPrimaryKeyTable();
			crawlerForeignKey.setPrimaryKeyTable(primaryKeyTable.getName());
			Table referencedTable = foreignKey.getReferencedTable();
			crawlerForeignKey.setReferencedTable(referencedTable.getName());
			Table referencingTable = foreignKey.getReferencingTable();
			crawlerForeignKey.setReferencingTable(referencingTable.getName());

			List<ColumnReference> columnReferences = foreignKey.getColumnReferences();
			if (ICollections.hasElements(columnReferences)) {
				crawlerForeignKey
					.setColumnReferences(columnReferences.stream()
						.map(t -> new CrawlerForeignKey.CrawlerColumnReference(t.getPrimaryKeyColumn().getName(),
							t.getForeignKeyColumn().getName(), t.getKeySequence()))
						.collect(Collectors.toList()));
			}
			List<TableConstraintColumn> constrainedColumns = foreignKey.getConstrainedColumns();
			if (ICollections.hasElements(constrainedColumns)) {
				crawlerForeignKey.setConstraintColumns(
					constrainedColumns.stream().map(t -> t.getName()).collect(Collectors.toList()));
			}
			return crawlerForeignKey;
		}
		return null;
	}

	public static List<CrawlerColumn> covertColumns(Collection<Column> columns) {
		if (ICollections.hasElements(columns)) {
			return columns.stream().sorted(Comparator.comparingInt(Column::getOrdinalPosition))
				.map(t -> covertColumn(t)).collect(Collectors.toList());
		}
		return Lists.newArrayList();
	}

	public static CrawlerColumn covertColumn(Column column) {
		CrawlerColumn crawlerColumn = copyProperties(column, new CrawlerColumn());
		crawlerColumn.setColumnDataType(column.getColumnDataType().getName());
		crawlerColumn.setJavaSqlType(column.getColumnDataType().getJavaSqlType().getName());

		crawlerColumn.setDecimalDigits(column.getDecimalDigits());
		crawlerColumn.setDefaultValue(column.getDefaultValue());
		crawlerColumn.setIsAutoIncremented(column.isAutoIncremented());
		crawlerColumn.setIsColumnDataTypeKnown(column.isColumnDataTypeKnown());
		crawlerColumn.setIsGenerated(column.isGenerated());
		crawlerColumn.setIsHidden(column.isHidden());
		crawlerColumn.setIsNullable(column.isNullable());
		crawlerColumn.setIsParentPartial(column.isParentPartial());
		crawlerColumn.setIsPartOfForeignKey(column.isPartOfForeignKey());
		crawlerColumn.setIsPartOfIndex(column.isPartOfIndex());
		crawlerColumn.setIsPartOfUniqueIndex(column.isPartOfUniqueIndex());
		crawlerColumn.setIsPartOfPrimaryKey(column.isPartOfPrimaryKey());
		crawlerColumn.setOrdinalPosition(column.getOrdinalPosition());
		crawlerColumn.setShortName(column.getShortName());
		crawlerColumn.setWidth(column.getWidth());
		crawlerColumn.setSize(column.getSize());
		return crawlerColumn;
	}

	private static <S extends NamedObject & DescribedObject, T extends BaseCrawlerEntity> T copyProperties(S s, T t) {
		String name = s.getName();
		String fullName = trimQuotationMarks(s.getFullName());

		if (StringUtils.isEmpty(name)) {
			name = fullName;
		}
		name = trimQuotationMarks(name);
		t.setName(name);
		t.setFullName(fullName);
		t.setRemarks(s.getRemarks());
		return t;
	}

	private static String trimQuotationMarks(String text) {
		return RegExUtils.removeAll(text, "\"");
	}

}
