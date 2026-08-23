import { describe, expect, it } from 'vitest';
import {
  ApiResearchRepository,
  PublicSampleResearchRepository,
  SyntheticFixtureResearchRepository,
} from '../domain/repository';
import { createDefaultRepository } from './research';

describe('createDefaultRepository', () => {
  it('uses the bundled public sample directly for static builds', () => {
    expect(createDefaultRepository('fixture')).toBeInstanceOf(PublicSampleResearchRepository);
    expect(createDefaultRepository('public-sample')).toBeInstanceOf(PublicSampleResearchRepository);
  });

  it('keeps API-first behavior for local and hosted application builds', () => {
    expect(createDefaultRepository(undefined)).toBeInstanceOf(ApiResearchRepository);
    expect(createDefaultRepository('api')).toBeInstanceOf(ApiResearchRepository);
  });

  it('keeps synthetic records behind an explicit regression mode', () => {
    expect(createDefaultRepository('synthetic')).toBeInstanceOf(SyntheticFixtureResearchRepository);
  });
});
