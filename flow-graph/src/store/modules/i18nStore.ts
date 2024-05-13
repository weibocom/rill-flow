import { defineStore } from 'pinia';
import { store } from '../index';
import { ref } from "vue";


export const useI18nStore = defineStore('i18n', () => {
  const i18nStore = ref();

  function setI18n(i18n) {
    i18nStore.value = i18n;
    const { t }= i18n.global
  }

  function getI18n() {
    return i18nStore.value;
  }

  return {
    setI18n,
    getI18n
  };
});

export function useI18nStoreWithOut() {
  return useI18nStore(store);
}
