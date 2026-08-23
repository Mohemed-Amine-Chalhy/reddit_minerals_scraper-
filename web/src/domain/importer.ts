import { researchRecordSchema, type ResearchRecord } from './schemas';

export const MAX_IMPORT_BYTES = 10 * 1024 * 1024;
const MAX_IMPORT_RECORDS = 5_000;

export class ImportValidationError extends Error {
  public constructor(message: string) {
    super(message);
    this.name = 'ImportValidationError';
  }
}

function validateRecord(value: unknown, location: string): ResearchRecord {
  const result = researchRecordSchema.safeParse(value);
  if (result.success) {
    return result.data;
  }
  const issue = result.error.issues[0];
  const field = issue?.path.length ? ` at ${issue.path.join('.')}` : '';
  throw new ImportValidationError(
    `${location}${field}: ${issue?.message ?? 'record does not match export schema v1'}`,
  );
}

function validateCount(records: readonly ResearchRecord[]): ResearchRecord[] {
  if (records.length === 0) {
    throw new ImportValidationError('The selected file contains no export records.');
  }
  if (records.length > MAX_IMPORT_RECORDS) {
    throw new ImportValidationError(`Imports are limited to ${MAX_IMPORT_RECORDS} records.`);
  }
  return [...records];
}

export function parseExportText(text: string): ResearchRecord[] {
  const bytes = new TextEncoder().encode(text).byteLength;
  if (bytes > MAX_IMPORT_BYTES) {
    throw new ImportValidationError('The selected file is larger than the 10 MB demo limit.');
  }
  const trimmed = text.trim();
  if (!trimmed) {
    throw new ImportValidationError('The selected file is empty.');
  }

  if (trimmed.startsWith('[') || trimmed.startsWith('{')) {
    try {
      const parsed: unknown = JSON.parse(trimmed);
      if (Array.isArray(parsed)) {
        return validateCount(
          parsed.map((item, index) => validateRecord(item, `Record ${index + 1}`)),
        );
      }
      if (typeof parsed === 'object' && parsed !== null && 'records' in parsed) {
        const records = parsed.records;
        if (!Array.isArray(records)) {
          throw new ImportValidationError('The JSON records property must be an array.');
        }
        return validateCount(
          records.map((item, index) => validateRecord(item, `Record ${index + 1}`)),
        );
      }
      return validateCount([validateRecord(parsed, 'Record 1')]);
    } catch (error) {
      if (error instanceof ImportValidationError) {
        throw error;
      }
      // A JSONL export also starts with "{". Fall through so validation can
      // identify the exact malformed line instead of returning a generic error.
    }
  }

  const records: ResearchRecord[] = [];
  for (const [index, line] of text.split(/\r?\n/u).entries()) {
    if (!line.trim()) continue;
    let value: unknown;
    try {
      value = JSON.parse(line);
    } catch {
      throw new ImportValidationError(`Line ${index + 1}: invalid JSON.`);
    }
    records.push(validateRecord(value, `Line ${index + 1}`));
  }
  return validateCount(records);
}
