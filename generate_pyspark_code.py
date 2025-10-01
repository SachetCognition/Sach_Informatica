"""
PySpark Code Generator from Informatica PowerCenter XML

This script uses the parser to generate PySpark ETL code from Informatica mappings.
It handles common transformation patterns:
- Source reading (CSV, database tables)
- Expressions (withColumn)
- Filters (filter/where)
- Aggregations (groupBy + agg)
- Joins (join)
- Lookups (broadcast joins)
- Targets (write operations)
"""

from infa_to_pyspark_parser import InformaticaXMLParser, Transformation
from typing import List, Dict, Set
import re


class PySparkCodeGenerator:
    """Generates PySpark code from parsed Informatica metadata"""
    
    def __init__(self, parser: InformaticaXMLParser):
        self.parser = parser
        self.indent_level = 0
        self.dataframe_counter = 0
        self.lookup_dfs: Dict[str, str] = {}  # lookup_name -> df_variable
    
    def _indent(self, lines: str) -> str:
        """Add indentation to code lines"""
        indent = "    " * self.indent_level
        return "\n".join(indent + line if line.strip() else "" for line in lines.split("\n"))
    
    def _get_df_name(self, transformation_name: str) -> str:
        """Generate a clean DataFrame variable name"""
        # Remove prefixes and clean up
        name = transformation_name.replace("SQ_", "").replace("EXPTRANS", "exp").replace("FILTRANS", "filtered")
        name = re.sub(r'[^a-zA-Z0-9_]', '_', name).lower()
        return f"df_{name}"
    
    def _map_datatype(self, infa_type: str) -> str:
        """Map Informatica datatypes to PySpark types"""
        type_mapping = {
            'string': 'StringType()',
            'decimal': 'DecimalType()',
            'number': 'DoubleType()',
            'double': 'DoubleType()',
            'integer': 'IntegerType()',
            'date': 'DateType()',
            'timestamp': 'TimestampType()',
            'varchar2': 'StringType()',
            'number(p,s)': 'DecimalType()'
        }
        return type_mapping.get(infa_type.lower(), 'StringType()')
    
    def _convert_expression(self, infa_expr: str) -> str:
        """Convert Informatica expression to PySpark expression"""
        if not infa_expr:
            return ""
        
        expr = infa_expr
        
        # Handle IIF (Informatica) -> when/otherwise (PySpark)
        iif_pattern = r'IIF\s*\((.*?),(.*?),(.*?)\)'
        if 'IIF' in expr:
            match = re.search(iif_pattern, expr)
            if match:
                condition, true_val, false_val = match.groups()
                expr = f"F.when({self._convert_condition(condition.strip())}, {true_val.strip()}).otherwise({false_val.strip()})"
        
        # Handle CONCAT -> concat
        expr = re.sub(r'CONCAT\s*\((.*?)\)', r'F.concat(\1)', expr)
        
        # Handle string functions
        expr = expr.replace('LTRIM', 'F.ltrim')
        expr = expr.replace('RTRIM', 'F.rtrim')
        expr = expr.replace('UPPER', 'F.upper')
        expr = expr.replace('LOWER', 'F.lower')
        
        # Handle aggregation functions
        expr = expr.replace('SUM(', 'F.sum(')
        expr = expr.replace('MAX(', 'F.max(')
        expr = expr.replace('MIN(', 'F.min(')
        expr = expr.replace('AVG(', 'F.avg(')
        expr = expr.replace('COUNT(', 'F.count(')
        expr = expr.replace('FIRST(', 'F.first(')
        
        # Handle column references - wrap in F.col()
        # Simple heuristic: if it's a single word (field name), wrap it
        if expr and not any(func in expr for func in ['F.', '(', ')', '+', '-', '*', '/']):
            expr = f'F.col("{expr}")'
        
        return expr
    
    def _convert_condition(self, condition: str) -> str:
        """Convert Informatica condition to PySpark condition"""
        cond = condition
        
        # Replace operators
        cond = cond.replace('=', '==')
        cond = cond.replace('<>', '!=')
        cond = cond.replace('AND', '&')
        cond = cond.replace('OR', '|')
        
        # Wrap field names in F.col()
        # This is simplified - a full parser would be better
        words = re.findall(r'\b[A-Za-z_][A-Za-z0-9_]*\b', cond)
        for word in words:
            if word not in ['True', 'False', 'None']:
                cond = cond.replace(word, f'F.col("{word}")')
        
        return cond
    
    def generate_imports(self) -> str:
        """Generate PySpark import statements"""
        code = """from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \\
    .appName("Informatica_to_PySpark_Conversion") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .getOrCreate()
"""
        return code
    
    def generate_source_read(self, source) -> tuple[str, str]:
        """Generate code to read a source"""
        df_name = self._get_df_name(source.name)
        
        if source.is_flat_file:
            delimiter = source.flat_file_config.get('delimiters', ',')
            skip_rows = source.flat_file_config.get('skip_rows', '0')
            
            code = f"""
# Read source: {source.name} (Flat File)
{df_name} = spark.read \\
    .option("header", "true") \\
    .option("delimiter", "{delimiter}") \\
    .option("inferSchema", "true") \\
    .csv("path/to/{source.name.lower()}.csv")
"""
        else:
            # Database source
            code = f"""
# Read source: {source.name} (Database: {source.database_type})
{df_name} = spark.read \\
    .format("jdbc") \\
    .option("url", "jdbc:oracle:thin:@//host:port/service") \\
    .option("dbtable", "{source.name}") \\
    .option("user", "username") \\
    .option("password", "password") \\
    .load()
"""
        
        return df_name, code
    
    def generate_expression_transform(self, trans: Transformation, input_df: str) -> tuple[str, str]:
        """Generate code for Expression transformation"""
        output_df = self._get_df_name(trans.name)
        
        code = f"\n# Expression transformation: {trans.name}\n"
        code += f"{output_df} = {input_df}"
        
        # Generate withColumn for each output field with expression
        for field in trans.fields:
            if field.expression and field.port_type in ['OUTPUT', 'INPUT/OUTPUT']:
                # Skip if expression is just the field name itself (pass-through)
                if field.expression.strip() == field.name:
                    continue
                
                pyspark_expr = self._convert_expression(field.expression)
                if pyspark_expr:
                    code += f' \\\n    .withColumn("{field.name}", {pyspark_expr})'
        
        return output_df, code
    
    def generate_filter_transform(self, trans: Transformation, input_df: str) -> tuple[str, str]:
        """Generate code for Filter transformation"""
        output_df = self._get_df_name(trans.name)
        
        filter_condition = trans.attributes.get('Filter Condition', '')
        
        # Handle unconnected lookups in filter
        if ':LKP.' in filter_condition:
            # This requires a join - simplified for now
            code = f"""
# Filter transformation: {trans.name}
# Note: Filter contains lookup - implement as join
{output_df} = {input_df}  # TODO: Implement lookup join for filter condition
"""
        else:
            pyspark_condition = self._convert_condition(filter_condition)
            code = f"""
# Filter transformation: {trans.name}
{output_df} = {input_df}.filter({pyspark_condition})
"""
        
        return output_df, code
    
    def generate_aggregator_transform(self, trans: Transformation, input_df: str) -> tuple[str, str]:
        """Generate code for Aggregator transformation"""
        output_df = self._get_df_name(trans.name)
        
        # Find group by fields (EXPRESSIONTYPE="GROUPBY")
        group_by_fields = []
        agg_expressions = []
        
        for field in trans.fields:
            if field.expression:
                if 'GROUPBY' in str(field.expression).upper() or \
                   (hasattr(field, 'expression_type') and field.expression == field.name):
                    group_by_fields.append(field.name)
                elif field.port_type == 'OUTPUT':
                    # This is an aggregation
                    pyspark_expr = self._convert_expression(field.expression)
                    agg_expressions.append(f'{pyspark_expr}.alias("{field.name}")')
        
        code = f"\n# Aggregator transformation: {trans.name}\n"
        
        if group_by_fields:
            group_by_str = ', '.join(f'"{f}"' for f in group_by_fields)
            code += f'{output_df} = {input_df}.groupBy({group_by_str})'
        else:
            code += f'{output_df} = {input_df}.groupBy()'
        
        if agg_expressions:
            agg_str = ',\n        '.join(agg_expressions)
            code += f' \\\n    .agg(\n        {agg_str}\n    )'
        
        return output_df, code
    
    def generate_joiner_transform(self, trans: Transformation, input_dfs: List[str]) -> tuple[str, str]:
        """Generate code for Joiner transformation"""
        output_df = self._get_df_name(trans.name)
        
        join_condition = trans.attributes.get('Join Condition', '')
        join_type = trans.attributes.get('Join Type', 'Normal Join')
        
        # Map Informatica join types to PySpark
        join_type_map = {
            'Normal Join': 'inner',
            'Master Outer Join': 'left',
            'Detail Outer Join': 'right',
            'Full Outer Join': 'outer'
        }
        pyspark_join_type = join_type_map.get(join_type, 'inner')
        
        if len(input_dfs) >= 2:
            pyspark_condition = self._convert_condition(join_condition)
            code = f"""
# Joiner transformation: {trans.name}
{output_df} = {input_dfs[0]}.join(
    {input_dfs[1]},
    {pyspark_condition},
    "{pyspark_join_type}"
)
"""
        else:
            code = f"# Joiner {trans.name} - insufficient inputs\n"
        
        return output_df, code
    
    def generate_sorter_transform(self, trans: Transformation, input_df: str) -> tuple[str, str]:
        """Generate code for Sorter transformation"""
        output_df = self._get_df_name(trans.name)
        
        # Find sort keys
        sort_fields = []
        for field in trans.fields:
            # Check if field has ISSORTKEY attribute (would need to parse XML attributes)
            # For now, assume first few fields are sort keys
            sort_fields.append(field.name)
        
        if sort_fields:
            sort_str = ', '.join(f'"{f}"' for f in sort_fields[:3])  # Limit to first 3
            code = f"""
# Sorter transformation: {trans.name}
{output_df} = {input_df}.orderBy({sort_str})
"""
        else:
            code = f"{output_df} = {input_df}  # No sort keys defined\n"
        
        return output_df, code
    
    def generate_target_write(self, target, input_df: str) -> str:
        """Generate code to write to target"""
        if target.is_flat_file:
            code = f"""
# Write to target: {target.name} (Flat File)
{input_df}.write \\
    .mode("overwrite") \\
    .option("header", "true") \\
    .csv("generated/outputs/{target.name.lower()}")
"""
        else:
            code = f"""
# Write to target: {target.name} (Database: {target.database_type})
# Note: Update connection details for actual database writes
# For testing, writing to CSV instead:
{input_df}.write \\
    .mode("overwrite") \\
    .option("header", "true") \\
    .csv("generated/outputs/{target.name.lower()}")

# Uncomment below for actual JDBC write:
# {input_df}.write \\
#     .format("jdbc") \\
#     .option("url", "jdbc:oracle:thin:@//host:port/service") \\
#     .option("dbtable", "{target.name}") \\
#     .option("user", "username") \\
#     .option("password", "password") \\
#     .mode("overwrite") \\
#     .save()
"""
        
        return code
    
    def generate_mapping_code(self, mapping_name: str = None) -> str:
        """Generate complete PySpark code for a mapping"""
        mapping = self.parser.mappings[0] if not mapping_name else next(
            (m for m in self.parser.mappings if m.name == mapping_name), None
        )
        
        if not mapping:
            return "# No mapping found"
        
        code = self.generate_imports()
        code += f"\n\n{'#' * 80}\n"
        code += f"# MAPPING: {mapping.name}\n"
        code += f"{'#' * 80}\n\n"
        
        # Track dataframes
        df_map = {}  # transformation_name -> df_variable
        
        # Step 1: Read all sources
        code += "# " + "=" * 76 + "\n"
        code += "# STEP 1: READ SOURCES\n"
        code += "# " + "=" * 76 + "\n"
        
        for source in self.parser.sources:
            df_name, source_code = self.generate_source_read(source)
            code += source_code
            df_map[f"SQ_{source.name}"] = df_name
        
        # Step 2: Process transformations in order
        code += "\n# " + "=" * 76 + "\n"
        code += "# STEP 2: TRANSFORMATIONS\n"
        code += "# " + "=" * 76 + "\n"
        
        # Build execution order from connectors
        processed = set(df_map.keys())
        
        for trans in mapping.transformations:
            if trans.name in processed:
                continue
            
            # Find input dataframe(s)
            input_conns = [c for c in mapping.connectors if c.to_instance == trans.name]
            if not input_conns:
                continue
            
            input_df = df_map.get(input_conns[0].from_instance)
            if not input_df:
                continue
            
            # Generate transformation code based on type
            if trans.type == 'Expression':
                output_df, trans_code = self.generate_expression_transform(trans, input_df)
            elif trans.type == 'Filter':
                output_df, trans_code = self.generate_filter_transform(trans, input_df)
            elif trans.type == 'Aggregator':
                output_df, trans_code = self.generate_aggregator_transform(trans, input_df)
            elif trans.type == 'Sorter':
                output_df, trans_code = self.generate_sorter_transform(trans, input_df)
            elif trans.type == 'Joiner':
                input_dfs = [df_map.get(c.from_instance) for c in input_conns if df_map.get(c.from_instance)]
                output_df, trans_code = self.generate_joiner_transform(trans, input_dfs)
            else:
                # Skip or handle other types
                output_df = input_df
                trans_code = f"\n# {trans.type}: {trans.name} (not implemented)\n{self._get_df_name(trans.name)} = {input_df}\n"
            
            code += trans_code
            df_map[trans.name] = output_df
            processed.add(trans.name)
        
        # Step 3: Write to targets
        code += "\n# " + "=" * 76 + "\n"
        code += "# STEP 3: WRITE TO TARGETS\n"
        code += "# " + "=" * 76 + "\n"
        
        for target in self.parser.targets:
            # Find which dataframe connects to this target
            target_conns = [c for c in mapping.connectors if c.to_instance == target.name]
            if target_conns:
                input_df = df_map.get(target_conns[0].from_instance)
                if input_df:
                    code += self.generate_target_write(target, input_df)
        
        code += "\n# Stop Spark session\nspark.stop()\n"
        
        return code


def main():
    """Main function to generate PySpark code"""
    import sys
    from pathlib import Path
    
    if len(sys.argv) < 2:
        print("Usage: python generate_pyspark_code.py <informatica_xml_file> [output_py_file]")
        print("\nExample:")
        print("  python generate_pyspark_code.py wf_m_PatternForChurnData.xml")
        print("  python generate_pyspark_code.py wf_m_PatternForChurnData.xml output_etl.py")
        print("\nDefault output: generated/pyspark/<mapping_name>.py")
        sys.exit(1)
    
    xml_file = sys.argv[1]
    
    # Default output to generated/pyspark/ directory
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    else:
        # Create default output path
        xml_path = Path(xml_file)
        output_dir = Path("generated/pyspark")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = str(output_dir / f"{xml_path.stem}_etl.py")
    
    # Parse the XML
    print(f"📖 Parsing Informatica XML: {xml_file}")
    parser = InformaticaXMLParser(xml_file)
    parser.parse()
    
    # Generate PySpark code
    print(f"⚙️  Generating PySpark code...")
    generator = PySparkCodeGenerator(parser)
    
    for mapping in parser.mappings:
        print(f"   - Processing mapping: {mapping.name}")
        pyspark_code = generator.generate_mapping_code(mapping.name)
        
        with open(output_file, 'w') as f:
            f.write(pyspark_code)
        print(f"✅ PySpark code written to: {output_file}")


if __name__ == "__main__":
    main()
