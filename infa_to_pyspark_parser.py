"""
Informatica PowerCenter XML to PySpark Converter - Preprocessing Script

This script parses Informatica PowerCenter XML export files and extracts:
- Sources (tables, flat files)
- Targets (output destinations)
- Transformations (expressions, filters, aggregators, joins, lookups)
- Data flow (connectors between transformations)
- Mapplets (reusable transformation logic)

The extracted metadata can then be used to generate PySpark ETL code.
"""

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from pathlib import Path
import json


@dataclass
class Field:
    """Represents a field/column in a source, target, or transformation"""
    name: str
    datatype: str
    precision: Optional[str] = None
    scale: Optional[str] = None
    nullable: Optional[str] = None
    description: Optional[str] = None
    expression: Optional[str] = None  # For calculated fields
    port_type: Optional[str] = None  # INPUT, OUTPUT, INPUT/OUTPUT


@dataclass
class Source:
    """Represents a data source (table or file)"""
    name: str
    database_type: str
    fields: List[Field] = field(default_factory=list)
    owner: Optional[str] = None
    is_flat_file: bool = False
    flat_file_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Target:
    """Represents a data target (table or file)"""
    name: str
    database_type: str
    fields: List[Field] = field(default_factory=list)
    is_flat_file: bool = False


@dataclass
class Transformation:
    """Represents a transformation (Expression, Filter, Aggregator, etc.)"""
    name: str
    type: str
    fields: List[Field] = field(default_factory=list)
    attributes: Dict[str, str] = field(default_factory=dict)
    description: Optional[str] = None


@dataclass
class Connector:
    """Represents data flow between transformations"""
    from_instance: str
    from_field: str
    from_type: str
    to_instance: str
    to_field: str
    to_type: str


@dataclass
class Mapplet:
    """Represents a reusable mapplet"""
    name: str
    transformations: List[Transformation] = field(default_factory=list)
    connectors: List[Connector] = field(default_factory=list)
    input_fields: List[Field] = field(default_factory=list)
    output_fields: List[Field] = field(default_factory=list)
    is_active: bool = False


@dataclass
class Mapping:
    """Represents a complete ETL mapping/workflow"""
    name: str
    transformations: List[Transformation] = field(default_factory=list)
    connectors: List[Connector] = field(default_factory=list)
    description: Optional[str] = None


class InformaticaXMLParser:
    """Parser for Informatica PowerCenter XML files"""
    
    def __init__(self, xml_file_path: str):
        self.xml_file = Path(xml_file_path)
        self.tree = ET.parse(xml_file_path)
        self.root = self.tree.getroot()
        
        # Parsed objects
        self.sources: List[Source] = []
        self.targets: List[Target] = []
        self.mapplets: List[Mapplet] = []
        self.mappings: List[Mapping] = []
        
    def parse(self):
        """Parse the entire XML file"""
        folder = self.root.find('.//FOLDER')
        if folder is None:
            raise ValueError("No FOLDER element found in XML")
        
        # Parse sources
        for source_elem in folder.findall('SOURCE'):
            self.sources.append(self._parse_source(source_elem))
        
        # Parse targets
        for target_elem in folder.findall('TARGET'):
            self.targets.append(self._parse_target(target_elem))
        
        # Parse mapplets
        for mapplet_elem in folder.findall('MAPPLET'):
            self.mapplets.append(self._parse_mapplet(mapplet_elem))
        
        # Parse mappings
        for mapping_elem in folder.findall('MAPPING'):
            self.mappings.append(self._parse_mapping(mapping_elem))
    
    def _parse_source(self, elem: ET.Element) -> Source:
        """Parse a SOURCE element"""
        source = Source(
            name=elem.get('NAME', ''),
            database_type=elem.get('DATABASETYPE', ''),
            owner=elem.get('OWNERNAME')
        )
        
        # Check if it's a flat file
        flatfile_elem = elem.find('FLATFILE')
        if flatfile_elem is not None:
            source.is_flat_file = True
            source.flat_file_config = {
                'delimited': flatfile_elem.get('DELIMITED'),
                'delimiters': flatfile_elem.get('DELIMITERS'),
                'skip_rows': flatfile_elem.get('SKIPROWS'),
                'quote_char': flatfile_elem.get('QUOTE_CHARACTER')
            }
        
        # Parse fields
        for field_elem in elem.findall('SOURCEFIELD'):
            source.fields.append(self._parse_field(field_elem))
        
        return source
    
    def _parse_target(self, elem: ET.Element) -> Target:
        """Parse a TARGET element"""
        target = Target(
            name=elem.get('NAME', ''),
            database_type=elem.get('DATABASETYPE', '')
        )
        
        # Check if it's a flat file
        if elem.find('FLATFILE') is not None:
            target.is_flat_file = True
        
        # Parse fields
        for field_elem in elem.findall('TARGETFIELD'):
            target.fields.append(self._parse_field(field_elem))
        
        return target
    
    def _parse_field(self, elem: ET.Element) -> Field:
        """Parse a field element (SOURCEFIELD, TARGETFIELD, or TRANSFORMFIELD)"""
        return Field(
            name=elem.get('NAME', ''),
            datatype=elem.get('DATATYPE', ''),
            precision=elem.get('PRECISION'),
            scale=elem.get('SCALE'),
            nullable=elem.get('NULLABLE'),
            description=elem.get('DESCRIPTION'),
            expression=elem.get('EXPRESSION'),
            port_type=elem.get('PORTTYPE')
        )
    
    def _parse_transformation(self, elem: ET.Element) -> Transformation:
        """Parse a TRANSFORMATION element"""
        trans = Transformation(
            name=elem.get('NAME', ''),
            type=elem.get('TYPE', ''),
            description=elem.get('DESCRIPTION')
        )
        
        # Parse fields
        for field_elem in elem.findall('TRANSFORMFIELD'):
            trans.fields.append(self._parse_field(field_elem))
        
        # Parse attributes
        for attr_elem in elem.findall('TABLEATTRIBUTE'):
            attr_name = attr_elem.get('NAME', '')
            attr_value = attr_elem.get('VALUE', '')
            trans.attributes[attr_name] = attr_value
        
        return trans
    
    def _parse_connector(self, elem: ET.Element) -> Connector:
        """Parse a CONNECTOR element"""
        return Connector(
            from_instance=elem.get('FROMINSTANCE', ''),
            from_field=elem.get('FROMFIELD', ''),
            from_type=elem.get('FROMINSTANCETYPE', ''),
            to_instance=elem.get('TOINSTANCE', ''),
            to_field=elem.get('TOFIELD', ''),
            to_type=elem.get('TOINSTANCETYPE', '')
        )
    
    def _parse_mapplet(self, elem: ET.Element) -> Mapplet:
        """Parse a MAPPLET element"""
        mapplet = Mapplet(
            name=elem.get('NAME', '')
        )
        
        # Parse transformations within mapplet
        for trans_elem in elem.findall('TRANSFORMATION'):
            trans = self._parse_transformation(trans_elem)
            mapplet.transformations.append(trans)
            
            # Identify input/output transformations
            if trans.type == 'Input Transformation':
                mapplet.input_fields = trans.fields
            elif trans.type == 'Output Transformation':
                mapplet.output_fields = trans.fields
        
        # Parse connectors
        for conn_elem in elem.findall('CONNECTOR'):
            mapplet.connectors.append(self._parse_connector(conn_elem))
        
        return mapplet
    
    def _parse_mapping(self, elem: ET.Element) -> Mapping:
        """Parse a MAPPING element"""
        mapping = Mapping(
            name=elem.get('NAME', ''),
            description=elem.get('DESCRIPTION')
        )
        
        # Parse transformations
        for trans_elem in elem.findall('TRANSFORMATION'):
            mapping.transformations.append(self._parse_transformation(trans_elem))
        
        # Parse connectors
        for conn_elem in elem.findall('CONNECTOR'):
            mapping.connectors.append(self._parse_connector(conn_elem))
        
        return mapping
    
    def get_data_flow(self, mapping_name: Optional[str] = None) -> List[List[str]]:
        """
        Extract data flow paths from source to target
        Returns list of transformation chains
        """
        mapping = self.mappings[0] if not mapping_name else next(
            (m for m in self.mappings if m.name == mapping_name), None
        )
        
        if not mapping:
            return []
        
        # Build adjacency list
        graph = {}
        for conn in mapping.connectors:
            if conn.from_instance not in graph:
                graph[conn.from_instance] = []
            graph[conn.from_instance].append(conn.to_instance)
        
        # Find source qualifiers (starting points)
        sources = [t.name for t in mapping.transformations 
                  if t.type == 'Source Qualifier']
        
        # Find all paths using DFS
        paths = []
        for source in sources:
            self._dfs_paths(graph, source, [source], paths)
        
        return paths
    
    def _dfs_paths(self, graph: Dict, node: str, path: List[str], all_paths: List, max_depth: int = 20):
        """DFS to find all paths from source to target"""
        # Prevent infinite recursion with depth limit
        if len(path) > max_depth:
            return
        
        # Limit total paths to prevent explosion
        if len(all_paths) > 100:
            return
        
        if node not in graph or not graph[node]:
            all_paths.append(path.copy())
            return
        
        for neighbor in graph[node]:
            if neighbor not in path:  # Avoid cycles
                path.append(neighbor)
                self._dfs_paths(graph, neighbor, path, all_paths, max_depth)
                path.pop()
    
    def extract_lookup_calls(self, mapping_name: Optional[str] = None) -> Dict[str, List[str]]:
        """
        Extract unconnected lookup calls from expressions
        Returns dict of {transformation_name: [lookup_calls]}
        """
        mapping = self.mappings[0] if not mapping_name else next(
            (m for m in self.mappings if m.name == mapping_name), None
        )
        
        if not mapping:
            return {}
        
        lookup_calls = {}
        for trans in mapping.transformations:
            calls = []
            for field in trans.fields:
                if field.expression and ':LKP.' in field.expression:
                    calls.append(field.expression)
            
            if calls:
                lookup_calls[trans.name] = calls
        
        return lookup_calls
    
    def to_dict(self) -> Dict:
        """Convert parsed data to dictionary for JSON export"""
        return {
            'file': str(self.xml_file),
            'sources': [
                {
                    'name': s.name,
                    'type': s.database_type,
                    'is_flat_file': s.is_flat_file,
                    'fields': [
                        {
                            'name': f.name,
                            'datatype': f.datatype,
                            'precision': f.precision,
                            'scale': f.scale
                        } for f in s.fields
                    ]
                } for s in self.sources
            ],
            'targets': [
                {
                    'name': t.name,
                    'type': t.database_type,
                    'is_flat_file': t.is_flat_file,
                    'fields': [
                        {
                            'name': f.name,
                            'datatype': f.datatype,
                            'precision': f.precision,
                            'scale': f.scale
                        } for f in t.fields
                    ]
                } for t in self.targets
            ],
            'mappings': [
                {
                    'name': m.name,
                    'transformations': [
                        {
                            'name': t.name,
                            'type': t.type,
                            'fields': [
                                {
                                    'name': f.name,
                                    'datatype': f.datatype,
                                    'expression': f.expression,
                                    'port_type': f.port_type
                                } for f in t.fields
                            ],
                            'attributes': t.attributes
                        } for t in m.transformations
                    ],
                    'data_flow': self.get_data_flow(m.name),
                    'lookup_calls': self.extract_lookup_calls(m.name)
                } for m in self.mappings
            ],
            'mapplets': [
                {
                    'name': mp.name,
                    'input_fields': [f.name for f in mp.input_fields],
                    'output_fields': [f.name for f in mp.output_fields],
                    'transformations': [t.name for t in mp.transformations]
                } for mp in self.mapplets
            ]
        }
    
    def print_summary(self):
        """Print a human-readable summary of the parsed XML"""
        print(f"\n{'='*80}")
        print(f"INFORMATICA XML ANALYSIS: {self.xml_file.name}")
        print(f"{'='*80}\n")
        
        print(f"📥 SOURCES ({len(self.sources)}):")
        for source in self.sources:
            file_type = "Flat File" if source.is_flat_file else "Database"
            print(f"  • {source.name} ({file_type} - {source.database_type})")
            print(f"    Fields: {len(source.fields)}")
        
        print(f"\n📤 TARGETS ({len(self.targets)}):")
        for target in self.targets:
            file_type = "Flat File" if target.is_flat_file else "Database"
            print(f"  • {target.name} ({file_type} - {target.database_type})")
            print(f"    Fields: {len(target.fields)}")
        
        print(f"\n🔧 MAPPLETS ({len(self.mapplets)}):")
        for mapplet in self.mapplets:
            print(f"  • {mapplet.name}")
            print(f"    Transformations: {len(mapplet.transformations)}")
        
        print(f"\n🗺️  MAPPINGS ({len(self.mappings)}):")
        for mapping in self.mappings:
            print(f"  • {mapping.name}")
            print(f"    Transformations: {len(mapping.transformations)}")
            print(f"    Connectors: {len(mapping.connectors)}")
            
            # Show transformation types
            trans_types = {}
            for trans in mapping.transformations:
                trans_types[trans.type] = trans_types.get(trans.type, 0) + 1
            
            print(f"    Transformation breakdown:")
            for t_type, count in sorted(trans_types.items()):
                print(f"      - {t_type}: {count}")
            
            # Show data flow
            paths = self.get_data_flow(mapping.name)
            if paths:
                print(f"    Data flow paths: {len(paths)}")
                # Show first 5 unique paths
                shown = 0
                for i, path in enumerate(paths, 1):
                    if shown >= 5:
                        break
                    print(f"      Path {i}: {' → '.join(path)}")
                    shown += 1
                if len(paths) > 5:
                    print(f"      ... and {len(paths) - 5} more paths")
            
            # Show lookup calls
            lookups = self.extract_lookup_calls(mapping.name)
            if lookups:
                print(f"    Unconnected lookups:")
                for trans_name, calls in lookups.items():
                    print(f"      - {trans_name}: {len(calls)} lookup(s)")


def main():
    """Main function to demonstrate usage"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python infa_to_pyspark_parser.py <informatica_xml_file> [output_json]")
        print("\nExample:")
        print("  python infa_to_pyspark_parser.py wf_m_PatternForChurnData.xml")
        print("  python infa_to_pyspark_parser.py wf_m_PatternForChurnData.xml custom_output.json")
        print("\nDefault output: generated/metadata/<filename>_metadata.json")
        sys.exit(1)
    
    xml_file = sys.argv[1]
    
    # Default output to generated/metadata/ directory
    if len(sys.argv) > 2:
        output_json = sys.argv[2]
    else:
        # Create default output path
        xml_path = Path(xml_file)
        output_dir = Path("generated/metadata")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_json = str(output_dir / f"{xml_path.stem}_metadata.json")
    
    # Parse the XML
    parser = InformaticaXMLParser(xml_file)
    parser.parse()
    
    # Print summary
    parser.print_summary()
    
    # Export to JSON
    data = parser.to_dict()
    with open(output_json, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"\n✅ Metadata exported to: {output_json}")
    
    print(f"\n{'='*80}\n")


if __name__ == "__main__":
    main()
