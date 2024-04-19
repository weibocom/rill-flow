export class Synchronization {
  conditions: string[];
  maxConcurrency: number;

  constructor(conditions: string[], maxConcurrency: number) {
    this.conditions = conditions;
    this.maxConcurrency = maxConcurrency;
  }
}
