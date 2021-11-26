import { Context, Strategy } from 'unleash-client';

export default class ApplicationNameStrategy extends Strategy {
    constructor() {
        super('appName');
    }

    isEnabled(parameters: any, context: Context): boolean {
        if (!parameters.appNames) {
            return false;
        }

        return parameters.appNames.split(/\s*,\s*/).includes(context.appName);
    }
}
