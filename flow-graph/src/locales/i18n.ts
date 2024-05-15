import { createI18n } from 'vue-i18n'
import messages from './index'
import { useFlowStoreWithOut } from '../store/modules/flowGraphStore';
import { useI18nStoreWithOut } from '../store/modules/i18nStore';
export function initI18n() {
  const i18n =  createI18n({
    silentTranslationWarn: true,
    globalInjection: true,
    legacy: false,
    locale: useFlowStoreWithOut().getLanguage(),
    messages,
  });
  useI18nStoreWithOut().setI18n(i18n);
  return i18n;
}

