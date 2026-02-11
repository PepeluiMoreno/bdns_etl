/**
 * Store Pinia para estado del sistema.
 *
 * Monitorea:
 * - Conexión con el backend
 * - Conexión con la base de datos
 * - Estado de inicialización de catálogos
 */
import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import axios from 'axios'

export const useSystemStore = defineStore('system', () => {
  // ==========================================
  // STATE
  // ==========================================

  const backendOnline = ref(false)
  const databaseOnline = ref(false)
  const catalogos = ref([])
  const catalogosInicializados = ref(false)
  const loading = ref(false)
  const seedingInProgress = ref(false)
  const lastCheck = ref(null)

  // ==========================================
  // GETTERS
  // ==========================================

  const catalogosFaltantes = computed(() => {
    return catalogos.value.filter(c => c.estado !== 'ok')
  })

  const catalogosOk = computed(() => {
    return catalogos.value.filter(c => c.estado === 'ok')
  })

  const totalCatalogos = computed(() => catalogos.value.length)

  // ==========================================
  // ACTIONS
  // ==========================================

  async function checkSystemStatus() {
    loading.value = true
    try {
      const response = await axios.get('/api/etl/system-status')
      const data = response.data

      backendOnline.value = data.backend === 'ok'
      databaseOnline.value = data.database === true
      catalogosInicializados.value = data.catalogos?.inicializados === true
      catalogos.value = data.catalogos?.detalle || []
      lastCheck.value = new Date().toISOString()
    } catch (err) {
      // Si falla la petición, el backend no está disponible
      backendOnline.value = false
      databaseOnline.value = false
      catalogosInicializados.value = false
      catalogos.value = []
    } finally {
      loading.value = false
    }
  }

  async function seedCatalogos() {
    seedingInProgress.value = true
    try {
      await axios.post('/api/etl/entities/seed', {
        entity_id: 'catalogos'
      })
    } catch (err) {
      console.error('Error al iniciar seeding de catálogos:', err)
      throw err
    } finally {
      seedingInProgress.value = false
    }
  }

  return {
    // State
    backendOnline,
    databaseOnline,
    catalogos,
    catalogosInicializados,
    loading,
    seedingInProgress,
    lastCheck,

    // Getters
    catalogosFaltantes,
    catalogosOk,
    totalCatalogos,

    // Actions
    checkSystemStatus,
    seedCatalogos
  }
})
