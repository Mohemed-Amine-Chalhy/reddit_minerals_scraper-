import { describe, expect, it } from 'vitest';
import { DEMO_RECORDS } from '../data/fixtures';
import { ImportValidationError, parseExportText } from './importer';

describe('parseExportText', () => {
  it('accepts JSON wrappers and JSONL exports', () => {
    const selected = DEMO_RECORDS.slice(0, 2);
    expect(
      parseExportText(JSON.stringify({ export_schema_version: 1, records: selected })),
    ).toHaveLength(2);
    expect(
      parseExportText(selected.map((record) => JSON.stringify(record)).join('\n')),
    ).toHaveLength(2);
  });

  it('reports the malformed JSONL line without echoing its content', () => {
    const text = `${JSON.stringify(DEMO_RECORDS[0])}\nnot-private-json`;
    expect(() => parseExportText(text)).toThrow(new ImportValidationError('Line 2: invalid JSON.'));
  });

  it('rejects empty and incompatible exports', () => {
    expect(() => parseExportText('  ')).toThrow('empty');
    expect(() => parseExportText('{"records":[]}')).toThrow('no export records');
    expect(() => parseExportText('{"record_type":"post"}')).toThrow('export_schema_version');
  });
});
