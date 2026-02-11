/**
 * Store Pinia para gestión de estado ETL
 *
 * Centraliza:
 * - Procesos activos
 * - Ejecuciones recientes
 * - Estadísticas globales
 * - Conexión WebSocket
 */
import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import axios from 'axios'

export const useETLStore = defineStore('etl', () => {
  // ==========================================
  // STATE
  // ==========================================

  // Procesos activos (running)
  const activeProcesses = ref([])

  // Ejecuciones recientes (historial)
  const recentExecutions = ref([])

  // Estadísticas globales
  const statistics = ref({
    total_executions: { seeding: 0, sync: 0, total: 0, completed: 0, failed: 0 },
    active_processes: 0,
    total_records: 0,
    success_rate: 0,
    last_successful: { seeding: null, sync: null }
  })

  // WebSocket
  const wsConnection = ref(null)
  const wsConnected = ref(false)
  const wsReconnectAttempts = ref(0)

  // Loading states
  const loading = ref(false)
  const error = ref(null)

  // Entidades ETL (para dashboard de seeding)
  const entities = ref([
    {
      id: 'catalogos',
      nombre: 'Catálogos de la aplicación',
      icono: '📚',
      descripcion: 'Tablas de referencia necesarias para la integridad de datos',
      progreso: 0,
      total_registros: 0,
      registros_procesados: 0,
      ultima_sync: null,
      estado: 'pending', // pending | seeding | syncing | complete | outdated | error
      cambios_pendientes: { nuevos: 0, actualizados: 0, borrados: 0 },
      auto_resync_eligible: false,
      atemporal: true, // No depende del ejercicio
      can_seed: true,
      seed_blocked_reason: null
    },
    {
      id: 'convocatorias',
      nombre: 'Convocatorias',
      icono: '📦',
      descripcion: 'Convocatorias de ayudas y subvenciones',
      progreso: 0,
      total_registros: 0,
      registros_procesados: 0,
      ultima_sync: null,
      estado: 'pending',
      cambios_pendientes: { nuevos: 0, actualizados: 0, borrados: 0 },
      auto_resync_eligible: false,
      can_seed: false,
      seed_blocked_reason: 'Verificando dependencias...'
    },
    {
      id: 'concesiones',
      nombre: 'Concesiones',
      icono: '💰',
      descripcion: 'Concesiones genéricas de ayudas',
      progreso: 0,
      total_registros: 0,
      registros_procesados: 0,
      ultima_sync: null,
      estado: 'pending',
      cambios_pendientes: { nuevos: 0, actualizados: 0, borrados: 0 },
      auto_resync_eligible: false,
      can_seed: false,
      seed_blocked_reason: 'Verificando dependencias...'
    },
    {
      id: 'minimis',
      nombre: 'De Minimis',
      icono: '📊',
      descripcion: 'Ayudas de régimen de minimis',
      progreso: 0,
      total_registros: 0,
      registros_procesados: 0,
      ultima_sync: null,
      estado: 'pending',
      cambios_pendientes: { nuevos: 0, actualizados: 0, borrados: 0 },
      auto_resync_eligible: false,
      can_seed: false,
      seed_blocked_reason: 'Verificando dependencias...'
    },
    {
      id: 'ayudas_estado',
      nombre: 'Ayudas de Estado',
      icono: '🏛️',
      descripcion: 'Ayudas de régimen de estado',
      progreso: 0,
      total_registros: 0,
      registros_procesados: 0,
      ultima_sync: null,
      estado: 'pending',
      cambios_pendientes: { nuevos: 0, actualizados: 0, borrados: 0 },
      auto_resync_eligible: false,
      can_seed: false,
      seed_blocked_reason: 'Verificando dependencias...'
    },
    {
      id: 'partidos_politicos',
      nombre: 'Partidos Políticos',
      icono: '🎯',
      descripcion: 'Concesiones a partidos políticos',
      progreso: 0,
      total_registros: 0,
      registros_procesados: 0,
      ultima_sync: null,
      estado: 'pending',
      cambios_pendientes: { nuevos: 0, actualizados: 0, borrados: 0 },
      auto_resync_eligible: false,
      can_seed: false,
      seed_blocked_reason: 'Verificando dependencias...'
    }
    // TODO: grandes_beneficiarios - deshabilitado temporalmente
    // {
    //   id: 'grandes_beneficiarios',
    //   nombre: 'Grandes Beneficiarios',
    //   icono: '👥',
    //   descripcion: 'Beneficiarios con ayuda superior al umbral',
    //   progreso: 0,
    //   total_registros: 0,
    //   registros_procesados: 0,
    //   ultima_sync: null,
    //   estado: 'pending',
    //   cambios_pendientes: { nuevos: 0, actualizados: 0, borrados: 0 },
    //   auto_resync_eligible: false,
    //   can_seed: false,
    //   seed_blocked_reason: 'Verificando dependencias...'
    // }
  ])

  // ==========================================
  // GETTERS
  // ==========================================

  const hasActiveProcesses = computed(() => activeProcesses.value.length > 0)

  const runningProcesses = computed(() =>
    activeProcesses.value.filter(p => p.status === 'running')
  )

  const failedProcesses = computed(() =>
    recentExecutions.value.filter(p => p.status === 'failed')
  )

  const completedToday = computed(() => {
    const today = new Date()
    today.setHours(0, 0, 0, 0)

    return recentExecutions.value.filter(p => {
      if (!p.finished_at) return false
      const finishDate = new Date(p.finished_at)
      return finishDate >= today && p.status === 'completed'
    }).length
  })


  // ==========================================
  // ACTIONS - Data Loading
  // ==========================================

  async function loadStatistics() {
    try {
      const response = await axios.get('/api/etl/statistics')
      statistics.value = response.data
    } catch (err) {
      console.error('Error loading statistics:', err)
      error.value = 'Error al cargar estadísticas'
    }
  }

  async function loadRecentExecutions(limit = 20) {
    try {
      const response = await axios.get('/api/etl/executions', {
        params: { limit }
      })

      // Actualizar con datos completos
      recentExecutions.value = response.data.map(exec => ({
        ...exec,
        records_processed: exec.stats?.records_processed || 0,
        records_inserted: exec.stats?.records_inserted || 0,
        records_updated: exec.stats?.records_updated || 0,
        records_errors: exec.stats?.records_errors || 0
      }))
    } catch (err) {
      console.error('Error loading recent executions:', err)
      error.value = 'Error al cargar ejecuciones'
    }
  }

  async function loadActiveProcesses() {
    try {
      const response = await axios.get('/api/etl/executions', {
        params: { status: 'running', limit: 10 }
      })

      activeProcesses.value = response.data.map(proc => ({
        ...proc,
        progress: proc.progress?.percentage || 0,
        records_processed: proc.stats?.records_processed || 0,
        records_inserted: proc.stats?.records_inserted || 0,
        records_updated: proc.stats?.records_updated || 0,
        records_errors: proc.stats?.records_errors || 0
      }))
    } catch (err) {
      console.error('Error loading active processes:', err)
      activeProcesses.value = []
    }
  }

  async function loadAll() {
    loading.value = true
    error.value = null

    try {
      await Promise.all([
        loadStatistics(),
        loadRecentExecutions(),
        loadActiveProcesses()
      ])
    } catch (err) {
      console.error('Error loading ETL data:', err)
      error.value = 'Error al cargar datos'
    } finally {
      loading.value = false
    }
  }

  // ==========================================
  // ACTIONS - WebSocket
  // ==========================================

  function connectWebSocket() {
    if (wsConnection.value?.readyState === WebSocket.OPEN) {
      console.log('WebSocket already connected')
      return
    }

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const wsUrl = `${protocol}//localhost:8001/api/etl/ws`

    try {
      wsConnection.value = new WebSocket(wsUrl)

      wsConnection.value.onopen = () => {
        console.log('✅ WebSocket connected')
        wsConnected.value = true
        wsReconnectAttempts.value = 0
      }

      wsConnection.value.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data)
          handleWebSocketMessage(data)
        } catch (err) {
          console.error('Error parsing WebSocket message:', err)
        }
      }

      wsConnection.value.onerror = (error) => {
        console.error('WebSocket error:', error)
        wsConnected.value = false
      }

      wsConnection.value.onclose = () => {
        console.log('WebSocket disconnected')
        wsConnected.value = false

        // Auto-reconnect con backoff exponencial
        if (wsReconnectAttempts.value < 10) {
          const delay = Math.min(1000 * Math.pow(2, wsReconnectAttempts.value), 30000)
          setTimeout(() => {
            wsReconnectAttempts.value++
            connectWebSocket()
          }, delay)
        }
      }
    } catch (err) {
      console.error('Error creating WebSocket:', err)
      wsConnected.value = false
    }
  }

  function disconnectWebSocket() {
    if (wsConnection.value) {
      wsConnection.value.close()
      wsConnection.value = null
      wsConnected.value = false
    }
  }

  function handleWebSocketMessage(data) {
    switch (data.type) {
      case 'stats_update':
        // Actualizar estadísticas
        if (data.data?.statistics) {
          statistics.value = data.data.statistics
        }

        // Actualizar ejecuciones recientes
        if (data.data?.recent_executions) {
          // Merge con datos existentes para mantener detalles
          data.data.recent_executions.forEach(newExec => {
            const index = recentExecutions.value.findIndex(
              e => e.execution_id === newExec.execution_id
            )

            if (index !== -1) {
              recentExecutions.value[index] = {
                ...recentExecutions.value[index],
                ...newExec,
                records_processed: newExec.records_processed || 0,
                records_inserted: newExec.records_inserted || 0,
                records_updated: newExec.records_updated || 0,
                records_errors: newExec.records_errors || 0
              }
            } else {
              recentExecutions.value.unshift({
                ...newExec,
                records_processed: newExec.records_processed || 0,
                records_inserted: newExec.records_inserted || 0,
                records_updated: newExec.records_updated || 0,
                records_errors: newExec.records_errors || 0
              })
            }
          })

          // Limitar a 20 ejecuciones
          if (recentExecutions.value.length > 20) {
            recentExecutions.value = recentExecutions.value.slice(0, 20)
          }
        }

        // Actualizar procesos activos directamente desde el WebSocket
        if (data.data?.active_processes) {
          activeProcesses.value = data.data.active_processes.map(proc => ({
            ...proc,
            progress: proc.progress || 0,
            records_processed: proc.records_processed || 0,
            records_inserted: proc.records_inserted || 0,
            records_updated: proc.records_updated || 0,
            records_errors: proc.records_errors || 0
          }))
        }
        break

      case 'process_update':
        // Actualizar proceso específico
        updateProcess(data.data)
        break

      case 'process_started':
        // Nuevo proceso iniciado
        addProcess(data.data)
        loadStatistics() // Actualizar contadores
        break

      case 'process_completed':
      case 'process_failed':
      case 'process_cancelled':
        // Proceso finalizado
        removeProcess(data.data.execution_id)
        loadStatistics() // Actualizar contadores
        loadRecentExecutions() // Actualizar historial
        break

      default:
        console.log('Unknown WebSocket message type:', data.type)
    }
  }

  function updateProcess(processData) {
    const index = activeProcesses.value.findIndex(
      p => p.execution_id === processData.execution_id
    )

    if (index !== -1) {
      activeProcesses.value[index] = {
        ...activeProcesses.value[index],
        ...processData,
        progress: processData.progress?.percentage || activeProcesses.value[index].progress,
        records_processed: processData.stats?.records_processed || 0,
        records_inserted: processData.stats?.records_inserted || 0,
        records_updated: processData.stats?.records_updated || 0,
        records_errors: processData.stats?.records_errors || 0
      }
    }
  }

  function addProcess(processData) {
    const exists = activeProcesses.value.some(
      p => p.execution_id === processData.execution_id
    )

    if (!exists) {
      activeProcesses.value.unshift({
        ...processData,
        progress: 0,
        records_processed: 0,
        records_inserted: 0,
        records_updated: 0,
        records_errors: 0
      })
    }
  }

  function removeProcess(executionId) {
    activeProcesses.value = activeProcesses.value.filter(
      p => p.execution_id !== executionId
    )
  }

  // ==========================================
  // ACTIONS - Process Management
  // ==========================================

  async function startSeeding(config) {
    try {
      const response = await axios.post('/api/etl/seeding/start', config)

      // Agregar a procesos activos inmediatamente
      addProcess({
        ...response.data,
        progress: 0,
        current_phase: 'Iniciando...',
        records_processed: 0
      })

      return response.data
    } catch (err) {
      console.error('Error starting seeding:', err)
      throw err
    }
  }

  async function stopExecution(executionId) {
    try {
      await axios.post(`/api/etl/execution/${executionId}/stop`)
      removeProcess(executionId)
      await loadStatistics()
    } catch (err) {
      console.error('Error stopping execution:', err)
      throw err
    }
  }

  async function deleteExecution(executionId) {
    try {
      await axios.delete(`/api/etl/execution/${executionId}`)
      await loadRecentExecutions()
      await loadStatistics()
    } catch (err) {
      console.error('Error deleting execution:', err)
      throw err
    }
  }

  // ==========================================
  // ACTIONS - Entities Management
  // ==========================================

  async function loadEntitiesStatus(year = null) {
    try {
      const params = {}
      if (year) params.year = year
      const response = await axios.get('/api/etl/entities/status', { params })

      response.data.forEach(entityData => {
        const entity = entities.value.find(e => e.id === entityData.id)
        if (entity) {
          Object.assign(entity, entityData)
        }
      })
    } catch (err) {
      console.error('Error loading entities status:', err)
    }
  }

  async function startEntitySeeding(entityId, year = null, replacingExecutionId = null) {
    const entity = entities.value.find(e => e.id === entityId)
    if (!entity) throw new Error(`Entidad ${entityId} no encontrada`)

    entity.estado = 'seeding'

    try {
      const payload = {
        entity_id: entityId,
        year: year
      }
      if (replacingExecutionId) {
        payload.replacing_execution_id = replacingExecutionId
      }
      const response = await axios.post('/api/etl/entities/seed', payload)

      Object.assign(entity, response.data)
      return response.data
    } catch (err) {
      entity.estado = 'error'
      throw err
    }
  }

  async function cleanTempFiles(entityId, year = null) {
    try {
      const params = {}
      if (year) params.year = year
      const response = await axios.post(`/api/etl/entities/${entityId}/clean-temp`, null, { params })
      return response.data
    } catch (err) {
      console.error('Error cleaning temp files:', err)
      throw err
    }
  }

  async function startEntitySync(entityId) {
    const entity = entities.value.find(e => e.id === entityId)
    if (!entity) throw new Error(`Entidad ${entityId} no encontrada`)

    entity.estado = 'syncing'

    try {
      const response = await axios.post('/api/etl/entities/sync', {
        entity_id: entityId
      })

      Object.assign(entity, response.data)
      return response.data
    } catch (err) {
      entity.estado = 'error'
      throw err
    }
  }


  // ==========================================
  // RETURN
  // ==========================================

  return {
    // State
    activeProcesses,
    recentExecutions,
    statistics,
    wsConnected,
    loading,
    error,
    entities,

    // Getters
    hasActiveProcesses,
    runningProcesses,
    failedProcesses,
    completedToday,
    // Actions
    loadStatistics,
    loadRecentExecutions,
    loadActiveProcesses,
    loadAll,
    connectWebSocket,
    disconnectWebSocket,
    startSeeding,
    stopExecution,
    deleteExecution,
    loadEntitiesStatus,
    startEntitySeeding,
    startEntitySync,
    cleanTempFiles,
  }
})
