<template>
  <div>
      <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <!-- Configuración Panel -->
        <div class="lg:col-span-1">
          <div class="bg-white rounded-lg shadow p-6 sticky top-6">
            <h2 class="text-lg font-medium text-gray-900 mb-4">Configuración</h2>

            <form @submit.prevent="startSeeding" class="space-y-4">
              <!-- Año(s) -->
              <div>
                <label class="block text-sm font-medium text-gray-700 mb-2">
                  Año(s) a procesar <span class="text-red-500">*</span>
                </label>

                <!-- Selector visual de años -->
                <div class="space-y-2">
                  <div class="flex items-center space-x-2">
                    <input
                      v-model.number="yearFrom"
                      type="number"
                      min="2000"
                      :max="new Date().getFullYear()"
                      :disabled="isRunning"
                      class="flex-1 px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-blue-500 focus:border-blue-500 disabled:bg-gray-100"
                      placeholder="Desde"
                    />
                    <span class="text-gray-500">→</span>
                    <input
                      v-model.number="yearTo"
                      type="number"
                      min="2000"
                      :max="new Date().getFullYear()"
                      :disabled="isRunning"
                      class="flex-1 px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-blue-500 focus:border-blue-500 disabled:bg-gray-100"
                      placeholder="Hasta"
                    />
                  </div>
                  <p class="text-xs text-gray-500">
                    Se procesarán {{ getYearCount() }} año(s): {{ getYearRange() }}
                  </p>
                </div>
              </div>

              <!-- Entidades -->
              <div>
                <label class="block text-sm font-medium text-gray-700 mb-2">
                  Entidades a procesar <span class="text-red-500">*</span>
                </label>
                <div class="space-y-2">
                  <!-- Entidades principales -->
                  <div class="pb-2 border-b border-gray-200">
                    <p class="text-xs font-medium text-gray-500 mb-2">PRINCIPALES</p>
                    <div class="space-y-2">
                      <label class="flex items-center">
                        <input
                          v-model="selectedEntities"
                          type="checkbox"
                          value="convocatorias"
                          :disabled="isRunning"
                          class="rounded border-gray-300 text-blue-600 focus:ring-blue-500 disabled:opacity-50"
                        />
                        <span class="ml-2 text-sm text-gray-700">Convocatorias</span>
                      </label>
                      <label class="flex items-center">
                        <input
                          v-model="selectedEntities"
                          type="checkbox"
                          value="concesiones"
                          :disabled="isRunning"
                          class="rounded border-gray-300 text-blue-600 focus:ring-blue-500 disabled:opacity-50"
                        />
                        <span class="ml-2 text-sm text-gray-700">Concesiones</span>
                      </label>
                      <label class="flex items-center">
                        <input
                          v-model="selectedEntities"
                          type="checkbox"
                          value="catalogos"
                          :disabled="isRunning"
                          class="rounded border-gray-300 text-blue-600 focus:ring-blue-500 disabled:opacity-50"
                        />
                        <span class="ml-2 text-sm text-gray-700">Catálogos</span>
                      </label>
                    </div>
                  </div>

                  <!-- Entidades especiales (retención 10 años) -->
                  <div>
                    <p class="text-xs font-medium text-gray-500 mb-2">RETENCIÓN EXTENDIDA (10 AÑOS)</p>
                    <div class="space-y-2">
                      <label class="flex items-center">
                        <input
                          v-model="selectedEntities"
                          type="checkbox"
                          value="minimis"
                          :disabled="isRunning"
                          class="rounded border-gray-300 text-purple-600 focus:ring-purple-500 disabled:opacity-50"
                        />
                        <span class="ml-2 text-sm text-gray-700">
                          Ayudas de Minimis
                          <span class="ml-1 text-xs text-purple-600">(10 años)</span>
                        </span>
                      </label>
                      <label class="flex items-center">
                        <input
                          v-model="selectedEntities"
                          type="checkbox"
                          value="ayudas_estado"
                          :disabled="isRunning"
                          class="rounded border-gray-300 text-purple-600 focus:ring-purple-500 disabled:opacity-50"
                        />
                        <span class="ml-2 text-sm text-gray-700">
                          Ayudas de Estado
                          <span class="ml-1 text-xs text-purple-600">(10 años)</span>
                        </span>
                      </label>
                      <label class="flex items-center">
                        <input
                          v-model="selectedEntities"
                          type="checkbox"
                          value="partidos_politicos"
                          :disabled="isRunning"
                          class="rounded border-gray-300 text-purple-600 focus:ring-purple-500 disabled:opacity-50"
                        />
                        <span class="ml-2 text-sm text-gray-700">
                          Partidos Políticos
                          <span class="ml-1 text-xs text-purple-600">(especial)</span>
                        </span>
                      </label>
                      <label class="flex items-center">
                        <input
                          v-model="selectedEntities"
                          type="checkbox"
                          value="grandes_beneficiarios"
                          :disabled="isRunning"
                          class="rounded border-gray-300 text-purple-600 focus:ring-purple-500 disabled:opacity-50"
                        />
                        <span class="ml-2 text-sm text-gray-700">
                          Grandes Beneficiarios
                          <span class="ml-1 text-xs text-purple-600">(umbral)</span>
                        </span>
                      </label>
                    </div>
                  </div>
                </div>
                <p v-if="selectedEntities.length === 0" class="mt-1 text-xs text-red-600">
                  Selecciona al menos una entidad
                </p>
                <div class="mt-2 p-2 bg-purple-50 border border-purple-200 rounded text-xs text-purple-700">
                  ℹ️ <strong>Importante:</strong> Minimis, Ayudas de Estado y Partidos Políticos contienen
                  datos con retención obligatoria de 10 años y pueden incluir registros no disponibles en los
                  endpoints regulares.
                </div>
              </div>

              <!-- Opciones avanzadas -->
              <div class="pt-4 border-t border-gray-200">
                <h3 class="text-sm font-medium text-gray-700 mb-3">Opciones Avanzadas</h3>

                <div class="space-y-3">
                  <label class="flex items-center">
                    <input
                      v-model="config.cleanup_before"
                      type="checkbox"
                      :disabled="isRunning"
                      class="rounded border-gray-300 text-blue-600 focus:ring-blue-500 disabled:opacity-50"
                    />
                    <span class="ml-2 text-sm text-gray-600">Limpiar datos existentes</span>
                  </label>

                  <label class="flex items-center">
                    <input
                      v-model="config.create_backup"
                      type="checkbox"
                      :disabled="isRunning"
                      class="rounded border-gray-300 text-blue-600 focus:ring-blue-500 disabled:opacity-50"
                    />
                    <span class="ml-2 text-sm text-gray-600">Crear backup antes</span>
                  </label>

                  <label class="flex items-center">
                    <input
                      v-model="config.parallel_processing"
                      type="checkbox"
                      :disabled="isRunning"
                      class="rounded border-gray-300 text-blue-600 focus:ring-blue-500 disabled:opacity-50"
                    />
                    <span class="ml-2 text-sm text-gray-600">Procesamiento paralelo</span>
                  </label>

                  <label class="flex items-center">
                    <input
                      v-model="config.run_in_background"
                      type="checkbox"
                      :disabled="isRunning"
                      class="rounded border-gray-300 text-blue-600 focus:ring-blue-500 disabled:opacity-50"
                    />
                    <span class="ml-2 text-sm text-gray-600">Ejecutar en segundo plano</span>
                  </label>
                </div>
              </div>

              <!-- Configuración de paralelización -->
              <div v-if="config.parallel_processing">
                <label class="block text-sm font-medium text-gray-700 mb-2">
                  Número de workers
                </label>
                <input
                  v-model.number="config.workers"
                  type="number"
                  min="1"
                  max="16"
                  :disabled="isRunning"
                  class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-blue-500 focus:border-blue-500 disabled:bg-gray-100"
                />
                <p class="mt-1 text-xs text-gray-500">Workers paralelos (1-16)</p>
              </div>

              <!-- Batch size -->
              <div v-if="config.parallel_processing">
                <label class="block text-sm font-medium text-gray-700 mb-2">
                  Tamaño de lote
                </label>
                <input
                  v-model.number="config.batch_size"
                  type="number"
                  min="100"
                  max="10000"
                  step="100"
                  :disabled="isRunning"
                  class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-blue-500 focus:border-blue-500 disabled:bg-gray-100"
                />
                <p class="mt-1 text-xs text-gray-500">Registros por lote (100-10000)</p>
              </div>

              <!-- Error handling -->
              <div v-if="error" class="p-3 bg-red-50 border border-red-200 rounded-md">
                <p class="text-sm text-red-700">{{ error }}</p>
              </div>

              <!-- Botones de acción -->
              <div class="pt-4 space-y-2">
                <button
                  v-if="!isRunning"
                  type="submit"
                  class="w-full px-4 py-2 text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 rounded-md focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                >
                  🚀 Iniciar Seeding
                </button>

                <button
                  v-else
                  type="button"
                  @click="cancelSeeding"
                  class="w-full px-4 py-2 text-sm font-medium text-white bg-red-600 hover:bg-red-700 rounded-md focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500"
                >
                  ⏹ Cancelar Proceso
                </button>
              </div>
            </form>

            <!-- Info -->
            <div class="mt-6 p-3 bg-blue-50 border border-blue-200 rounded-md">
              <h4 class="text-xs font-medium text-blue-800 mb-1">ℹ️ Información</h4>
              <ul class="text-xs text-blue-700 space-y-1">
                <li>• El seeding puede tardar varios minutos</li>
                <li>• Se descargará el año completo de datos</li>
                <li>• Puedes seguir el progreso en tiempo real</li>
              </ul>
            </div>
          </div>
        </div>

        <!-- Estado y Logs Panel -->
        <div class="lg:col-span-2 space-y-6">
          <!-- Estado del proceso -->
          <div v-if="execution" class="bg-white rounded-lg shadow p-6">
            <div class="flex items-center justify-between mb-4">
              <h2 class="text-lg font-medium text-gray-900">Estado del Proceso</h2>
              <div class="flex items-center space-x-3">
                <button
                  @click="showDetails = !showDetails"
                  class="px-3 py-1 text-xs font-medium text-blue-700 bg-blue-50 hover:bg-blue-100 rounded-md transition-colors"
                >
                  {{ showDetails ? '📊 Ocultar detalles' : '📊 Ver detalles' }}
                </button>
                <span :class="getStatusClass(execution.status)" class="px-3 py-1 text-xs font-medium rounded-full">
                  {{ getStatusLabel(execution.status) }}
                </span>
              </div>
            </div>

            <!-- Entrypoint activo -->
            <div v-if="execution.entrypoint" class="mb-4 p-3 bg-gradient-to-r from-indigo-50 to-purple-50 border border-indigo-200 rounded-lg">
              <div class="flex items-center space-x-2">
                <span class="text-indigo-600">🔧</span>
                <div class="flex-1">
                  <span class="text-xs font-medium text-indigo-800">Script en ejecución:</span>
                  <code class="ml-2 text-sm font-mono text-indigo-900 bg-white px-2 py-0.5 rounded">
                    {{ execution.entrypoint }}
                  </code>
                </div>
              </div>
            </div>

            <!-- Progress bar -->
            <div v-if="execution.progress !== undefined" class="mb-4">
              <div class="flex justify-between text-sm text-gray-600 mb-2">
                <span>Progreso</span>
                <span>{{ execution.progress }}%</span>
              </div>
              <div class="w-full bg-gray-200 rounded-full h-3 overflow-hidden">
                <div
                  class="h-full bg-gradient-to-r from-blue-500 to-blue-600 transition-all duration-500 ease-out"
                  :style="{ width: `${execution.progress}%` }"
                ></div>
              </div>
            </div>

            <!-- Estadísticas principales -->
            <div class="mb-6">
              <h3 class="text-sm font-medium text-gray-700 mb-3">📈 Estadísticas del Proceso</h3>
              <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div class="bg-gradient-to-br from-blue-50 to-blue-100 rounded-lg p-4 border border-blue-200">
                  <div class="flex items-center justify-between mb-1">
                    <span class="text-xs font-medium text-blue-700">Total Procesados</span>
                    <span class="text-blue-500">📊</span>
                  </div>
                  <div class="text-2xl font-bold text-blue-900">{{ formatNumber(execution.records_processed) }}</div>
                  <div class="text-xs text-blue-600 mt-1">registros</div>
                </div>

                <div class="bg-gradient-to-br from-green-50 to-green-100 rounded-lg p-4 border border-green-200">
                  <div class="flex items-center justify-between mb-1">
                    <span class="text-xs font-medium text-green-700">Insertados</span>
                    <span class="text-green-500">✓</span>
                  </div>
                  <div class="text-2xl font-bold text-green-900">{{ formatNumber(execution.records_inserted) }}</div>
                  <div class="text-xs text-green-600 mt-1">nuevos</div>
                </div>

                <div class="bg-gradient-to-br from-yellow-50 to-yellow-100 rounded-lg p-4 border border-yellow-200">
                  <div class="flex items-center justify-between mb-1">
                    <span class="text-xs font-medium text-yellow-700">Actualizados</span>
                    <span class="text-yellow-500">↻</span>
                  </div>
                  <div class="text-2xl font-bold text-yellow-900">{{ formatNumber(execution.records_updated || 0) }}</div>
                  <div class="text-xs text-yellow-600 mt-1">modificados</div>
                </div>

                <div class="bg-gradient-to-br from-red-50 to-red-100 rounded-lg p-4 border border-red-200">
                  <div class="flex items-center justify-between mb-1">
                    <span class="text-xs font-medium text-red-700">Errores</span>
                    <span class="text-red-500">✗</span>
                  </div>
                  <div class="text-2xl font-bold text-red-900">{{ formatNumber(execution.records_errors || execution.records_failed || 0) }}</div>
                  <div class="text-xs text-red-600 mt-1">fallidos</div>
                </div>
              </div>

              <!-- Métricas de tiempo y velocidad -->
              <div class="grid grid-cols-3 gap-4 mt-4">
                <div class="bg-gray-50 rounded-lg p-3 border border-gray-200 text-center">
                  <div class="text-sm font-medium text-gray-700">⏱ Tiempo Transcurrido</div>
                  <div class="text-xl font-bold text-gray-900 mt-1">{{ execution.elapsed_time || 0 }}s</div>
                </div>

                <div class="bg-gray-50 rounded-lg p-3 border border-gray-200 text-center">
                  <div class="text-sm font-medium text-gray-700">⚡ Velocidad</div>
                  <div class="text-xl font-bold text-gray-900 mt-1">{{ calculateSpeed() }}</div>
                  <div class="text-xs text-gray-500">registros/seg</div>
                </div>

                <div class="bg-gray-50 rounded-lg p-3 border border-gray-200 text-center">
                  <div class="text-sm font-medium text-gray-700">🎯 Tasa de Éxito</div>
                  <div class="text-xl font-bold text-gray-900 mt-1">{{ calculateSuccessRate() }}%</div>
                </div>
              </div>
            </div>

            <!-- Información adicional -->
            <div class="grid grid-cols-2 gap-4 text-sm">
              <div>
                <span class="text-gray-600">ID Ejecución:</span>
                <code class="ml-2 text-xs bg-gray-100 px-2 py-1 rounded">{{ execution.execution_id }}</code>
              </div>
              <div>
                <span class="text-gray-600">Iniciado:</span>
                <span class="ml-2 text-gray-900">{{ formatDate(execution.started_at) }}</span>
              </div>
              <div>
                <span class="text-gray-600">Año:</span>
                <span class="ml-2 font-medium text-gray-900">{{ execution.year }}</span>
              </div>
              <div>
                <span class="text-gray-600">Entidad:</span>
                <span class="ml-2 font-medium text-gray-900">{{ execution.entity }}</span>
              </div>
            </div>

            <!-- Mensaje de finalización -->
            <div v-if="execution.status === 'completed'" class="mt-4 p-3 bg-green-50 border border-green-200 rounded-md">
              <p class="text-sm text-green-700">
                ✅ Seeding completado exitosamente en {{ execution.elapsed_time }}s
              </p>
            </div>

            <div v-if="execution.status === 'failed'" class="mt-4 p-3 bg-red-50 border border-red-200 rounded-md">
              <p class="text-sm text-red-700">
                ❌ El proceso falló: {{ execution.error_message }}
              </p>
            </div>

            <!-- Panel de detalles expandible -->
            <div v-if="showDetails" class="mt-6 pt-6 border-t border-gray-200">
              <h3 class="text-sm font-medium text-gray-900 mb-4">📋 Detalles de Ejecución</h3>

              <!-- Fase actual -->
              <div v-if="execution.current_phase" class="mb-6">
                <div class="flex items-center justify-between mb-2">
                  <span class="text-sm font-medium text-gray-700">Fase actual</span>
                  <span class="text-xs text-gray-500">{{ execution.current_phase }}</span>
                </div>
                <div class="relative">
                  <div class="flex justify-between mb-1">
                    <span
                      v-for="phase in phases"
                      :key="phase.id"
                      :class="getPhaseClass(phase.id)"
                      class="text-xs font-medium px-2 py-1 rounded"
                    >
                      {{ phase.label }}
                    </span>
                  </div>
                  <div class="w-full bg-gray-200 rounded-full h-2">
                    <div
                      class="h-full bg-blue-600 rounded-full transition-all duration-300"
                      :style="{ width: `${getPhaseProgress()}%` }"
                    ></div>
                  </div>
                </div>
              </div>

              <!-- Métricas por entidad -->
              <div v-if="execution.entity_metrics" class="mb-6">
                <h4 class="text-xs font-medium text-gray-700 mb-3">Métricas por Entidad</h4>
                <div class="space-y-3">
                  <div
                    v-for="(metrics, entity) in execution.entity_metrics"
                    :key="entity"
                    class="bg-gray-50 rounded-lg p-3"
                  >
                    <div class="flex items-center justify-between mb-2">
                      <span class="text-sm font-medium text-gray-900">{{ entity }}</span>
                      <span class="text-xs text-gray-500">{{ metrics.status || 'pending' }}</span>
                    </div>
                    <div class="grid grid-cols-3 gap-2 text-xs">
                      <div>
                        <span class="text-gray-600">Extraídos:</span>
                        <span class="ml-1 font-medium">{{ formatNumber(metrics.extracted || 0) }}</span>
                      </div>
                      <div>
                        <span class="text-gray-600">Procesados:</span>
                        <span class="ml-1 font-medium">{{ formatNumber(metrics.processed || 0) }}</span>
                      </div>
                      <div>
                        <span class="text-gray-600">Insertados:</span>
                        <span class="ml-1 font-medium">{{ formatNumber(metrics.inserted || 0) }}</span>
                      </div>
                    </div>
                    <div v-if="metrics.current_batch" class="mt-2 text-xs text-gray-500">
                      Lote actual: {{ metrics.current_batch }} / {{ metrics.total_batches || '?' }}
                    </div>
                  </div>
                </div>
              </div>

              <!-- Archivos generados -->
              <div v-if="execution.files_generated && execution.files_generated.length > 0" class="mb-6">
                <h4 class="text-xs font-medium text-gray-700 mb-3">📁 Archivos Generados</h4>
                <div class="space-y-2">
                  <div
                    v-for="(file, index) in execution.files_generated"
                    :key="index"
                    class="flex items-center justify-between bg-gray-50 rounded px-3 py-2 text-xs"
                  >
                    <div class="flex items-center space-x-2">
                      <span class="text-gray-400">📄</span>
                      <code class="text-gray-700">{{ file.name }}</code>
                    </div>
                    <span class="text-gray-500">{{ formatBytes(file.size) }}</span>
                  </div>
                </div>
              </div>

              <!-- Operación actual -->
              <div v-if="execution.current_operation" class="mb-6">
                <h4 class="text-xs font-medium text-gray-700 mb-2">🔄 Operación Actual</h4>
                <div class="bg-blue-50 border border-blue-200 rounded-lg p-3">
                  <p class="text-sm text-blue-900 font-mono">{{ execution.current_operation }}</p>
                  <div v-if="execution.operation_progress" class="mt-2">
                    <div class="flex justify-between text-xs text-blue-700 mb-1">
                      <span>Progreso de operación</span>
                      <span>{{ execution.operation_progress }}%</span>
                    </div>
                    <div class="w-full bg-blue-200 rounded-full h-1.5">
                      <div
                        class="h-full bg-blue-600 rounded-full"
                        :style="{ width: `${execution.operation_progress}%` }"
                      ></div>
                    </div>
                  </div>
                </div>
              </div>

              <!-- Estadísticas de rendimiento -->
              <div v-if="execution.performance_stats" class="mb-6">
                <h4 class="text-xs font-medium text-gray-700 mb-3">⚡ Rendimiento</h4>
                <div class="grid grid-cols-2 md:grid-cols-4 gap-3">
                  <div class="bg-gray-50 rounded-lg p-3 text-center">
                    <div class="text-lg font-bold text-gray-900">
                      {{ execution.performance_stats.records_per_second || 0 }}
                    </div>
                    <div class="text-xs text-gray-600">registros/seg</div>
                  </div>
                  <div class="bg-gray-50 rounded-lg p-3 text-center">
                    <div class="text-lg font-bold text-gray-900">
                      {{ execution.performance_stats.memory_usage || 0 }}
                    </div>
                    <div class="text-xs text-gray-600">MB memoria</div>
                  </div>
                  <div class="bg-gray-50 rounded-lg p-3 text-center">
                    <div class="text-lg font-bold text-gray-900">
                      {{ execution.performance_stats.cpu_usage || 0 }}%
                    </div>
                    <div class="text-xs text-gray-600">CPU</div>
                  </div>
                  <div class="bg-gray-50 rounded-lg p-3 text-center">
                    <div class="text-lg font-bold text-gray-900">
                      {{ execution.performance_stats.eta || 'N/A' }}
                    </div>
                    <div class="text-xs text-gray-600">ETA</div>
                  </div>
                </div>
              </div>

              <!-- Últimas operaciones -->
              <div v-if="execution.recent_operations && execution.recent_operations.length > 0">
                <h4 class="text-xs font-medium text-gray-700 mb-3">📝 Últimas Operaciones</h4>
                <div class="bg-gray-50 rounded-lg p-3 max-h-40 overflow-y-auto">
                  <div
                    v-for="(op, index) in execution.recent_operations"
                    :key="index"
                    class="text-xs text-gray-700 py-1 border-b border-gray-200 last:border-0"
                  >
                    <span class="text-gray-400 mr-2">{{ formatTime(new Date(op.timestamp)) }}</span>
                    <span>{{ op.description }}</span>
                  </div>
                </div>
              </div>

              <!-- Warnings/Alerts -->
              <div v-if="execution.warnings && execution.warnings.length > 0" class="mt-4">
                <div class="bg-yellow-50 border border-yellow-200 rounded-lg p-3">
                  <h4 class="text-xs font-medium text-yellow-800 mb-2">⚠️ Advertencias</h4>
                  <ul class="space-y-1">
                    <li
                      v-for="(warning, index) in execution.warnings"
                      :key="index"
                      class="text-xs text-yellow-700"
                    >
                      • {{ warning }}
                    </li>
                  </ul>
                </div>
              </div>
            </div>
          </div>

          <!-- Logs en tiempo real -->
          <div class="bg-white rounded-lg shadow">
            <div class="px-6 py-4 border-b border-gray-200 flex justify-between items-center">
              <h2 class="text-lg font-medium text-gray-900">Logs en Tiempo Real</h2>
              <button
                @click="clearLogs"
                class="px-3 py-1 text-xs font-medium text-gray-700 bg-gray-100 hover:bg-gray-200 rounded-md"
              >
                Limpiar
              </button>
            </div>
            <div class="p-4">
              <div
                ref="logsContainer"
                class="bg-gray-900 rounded-md p-4 h-96 overflow-y-auto font-mono text-xs text-gray-100"
              >
                <div v-if="logs.length === 0" class="text-gray-500 text-center py-8">
                  No hay logs disponibles. Inicia un proceso de seeding para ver los logs en tiempo real.
                </div>
                <div v-for="(log, index) in logs" :key="index" class="mb-1">
                  <span :class="getLogClass(log.level)" class="inline-block w-16">
                    [{{ log.level.toUpperCase() }}]
                  </span>
                  <span class="text-gray-400 mr-2">{{ formatTime(log.timestamp) }}</span>
                  <span>{{ log.message }}</span>
                </div>
              </div>
            </div>
          </div>

          <!-- Historial reciente -->
          <div v-if="recentExecutions.length > 0" class="bg-white rounded-lg shadow">
            <div class="px-6 py-4 border-b border-gray-200">
              <h2 class="text-lg font-medium text-gray-900">Historial Reciente</h2>
            </div>
            <div class="overflow-x-auto">
              <table class="min-w-full divide-y divide-gray-200">
                <thead class="bg-gray-50">
                  <tr>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Fecha</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Año</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Entidad</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Estado</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Registros</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Tiempo</th>
                  </tr>
                </thead>
                <tbody class="bg-white divide-y divide-gray-200">
                  <tr v-for="exec in recentExecutions" :key="exec.execution_id" class="hover:bg-gray-50">
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                      {{ formatDate(exec.started_at) }}
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {{ exec.year }}
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {{ exec.entity }}
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap">
                      <span :class="getStatusClass(exec.status)" class="px-2 py-1 text-xs rounded-full">
                        {{ getStatusLabel(exec.status) }}
                      </span>
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {{ formatNumber(exec.records_processed) }}
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {{ exec.elapsed_time }}s
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, nextTick, computed } from 'vue'
import { useETLStore } from '@/stores/etl'
import axios from 'axios'

// Store
const etlStore = useETLStore()

// Estado del formulario
const yearFrom = ref(new Date().getFullYear())
const yearTo = ref(new Date().getFullYear())
const selectedEntities = ref(['convocatorias'])

const config = ref({
  cleanup_before: false,
  create_backup: true,
  parallel_processing: true,
  batch_size: 1000,
  workers: 6,
  run_in_background: false
})

// Estado de la ejecución
const execution = ref(null)
const isRunning = ref(false)
const error = ref(null)
const showDetails = ref(false)

// Logs
const logs = ref([])
const logsContainer = ref(null)

// Usar historial del store
const recentExecutions = computed(() => etlStore.recentExecutions.slice(0, 10))

onMounted(() => {
  // Cargar datos del store
  etlStore.loadRecentExecutions()

  // El WebSocket ya está conectado desde el App o Dashboard
  if (!etlStore.wsConnected) {
    etlStore.connectWebSocket()
  }

  // Escuchar actualizaciones del proceso activo
  watchStoreForExecutionUpdates()
})

onUnmounted(() => {
  // No desconectamos el WebSocket porque lo gestiona el store globalmente
})

// Monitorear actualizaciones del store para el proceso actual
function watchStoreForExecutionUpdates() {
  // Observar cambios en los procesos activos del store
  setInterval(() => {
    if (execution.value && isRunning.value) {
      const activeProcess = etlStore.activeProcesses.find(
        p => p.execution_id === execution.value.execution_id
      )

      if (activeProcess) {
        // Actualizar execution con datos del store
        execution.value = {
          ...execution.value,
          ...activeProcess,
          progress: activeProcess.progress || 0,
          records_processed: activeProcess.records_processed || 0,
          records_inserted: activeProcess.records_inserted || 0,
          records_updated: activeProcess.records_updated || 0,
          records_errors: activeProcess.records_errors || 0
        }
      } else if (execution.value.status !== 'running') {
        // El proceso terminó
        isRunning.value = false
      }
    }
  }, 1000)
}

// Iniciar seeding
async function startSeeding() {
  error.value = null

  // Validar entidades seleccionadas
  if (selectedEntities.value.length === 0) {
    error.value = 'Debes seleccionar al menos una entidad'
    return
  }

  // Validar rango de años
  if (!yearFrom.value || !yearTo.value) {
    error.value = 'Debes especificar el rango de años'
    return
  }

  if (yearFrom.value > yearTo.value) {
    error.value = 'El año inicial no puede ser mayor que el año final'
    return
  }

  try {
    // Construir payload con las nuevas opciones
    const payload = {
      year: yearFrom.value, // Por ahora solo el primer año
      entity: selectedEntities.value[0], // Por ahora solo la primera entidad
      ...config.value
    }

    // Usar el store para iniciar el seeding
    const response = await etlStore.startSeeding(payload)

    execution.value = {
      ...response,
      progress: 0,
      records_processed: 0,
      records_inserted: 0,
      records_updated: 0,
      records_errors: 0,
      entrypoint: 'seeding/run_etl.py' // Placeholder
    }

    isRunning.value = true
    logs.value = []

    const yearRange = yearFrom.value === yearTo.value ? yearFrom.value : `${yearFrom.value}-${yearTo.value}`
    addLog('info', `Seeding iniciado: ${selectedEntities.value.join(', ')} (${yearRange})`)
    addLog('info', `ID de ejecución: ${execution.value.execution_id}`)

  } catch (err) {
    error.value = err.response?.data?.detail || 'Error al iniciar el seeding'
    addLog('error', error.value)
  }
}

// Cancelar seeding
async function cancelSeeding() {
  if (!execution.value) return

  try {
    await etlStore.stopExecution(execution.value.execution_id)
    addLog('warning', 'Proceso cancelado')
    isRunning.value = false
  } catch (err) {
    error.value = 'Error al cancelar el proceso'
    addLog('error', error.value)
  }
}

// Añadir log
function addLog(level, message) {
  logs.value.push({
    level,
    message,
    timestamp: new Date()
  })

  // Auto-scroll al final
  nextTick(() => {
    if (logsContainer.value) {
      logsContainer.value.scrollTop = logsContainer.value.scrollHeight
    }
  })

  // Limitar logs a 500 líneas
  if (logs.value.length > 500) {
    logs.value = logs.value.slice(-500)
  }
}

function clearLogs() {
  logs.value = []
}

// Utilidades
function getStatusClass(status) {
  const classes = {
    running: 'bg-blue-100 text-blue-800',
    completed: 'bg-green-100 text-green-800',
    failed: 'bg-red-100 text-red-800',
    cancelled: 'bg-gray-100 text-gray-800',
    pending: 'bg-yellow-100 text-yellow-800'
  }
  return classes[status] || 'bg-gray-100 text-gray-800'
}

function getStatusLabel(status) {
  const labels = {
    running: 'En ejecución',
    completed: 'Completado',
    failed: 'Fallido',
    cancelled: 'Cancelado',
    pending: 'Pendiente'
  }
  return labels[status] || status
}

function getLogClass(level) {
  const classes = {
    info: 'text-blue-400',
    warning: 'text-yellow-400',
    error: 'text-red-400',
    success: 'text-green-400'
  }
  return classes[level] || 'text-gray-400'
}

function formatNumber(num) {
  if (!num) return 0
  return new Intl.NumberFormat('es-ES').format(num)
}

function formatDate(dateString) {
  if (!dateString) return 'N/A'
  return new Date(dateString).toLocaleString('es-ES', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  })
}

function formatTime(date) {
  return date.toLocaleTimeString('es-ES', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  })
}

function formatBytes(bytes) {
  if (!bytes) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i]
}

// Fases del proceso
const phases = [
  { id: 'extracting', label: 'Extracción' },
  { id: 'transforming', label: 'Transformación' },
  { id: 'loading', label: 'Carga' },
  { id: 'validating', label: 'Validación' }
]

function getPhaseClass(phaseId) {
  if (!execution.value?.current_phase) return 'bg-gray-200 text-gray-600'

  const currentIndex = phases.findIndex(p => p.id === execution.value.current_phase)
  const phaseIndex = phases.findIndex(p => p.id === phaseId)

  if (phaseIndex < currentIndex) return 'bg-green-100 text-green-700'
  if (phaseIndex === currentIndex) return 'bg-blue-100 text-blue-700'
  return 'bg-gray-100 text-gray-500'
}

function getPhaseProgress() {
  if (!execution.value?.current_phase) return 0

  const currentIndex = phases.findIndex(p => p.id === execution.value.current_phase)
  return ((currentIndex + 1) / phases.length) * 100
}

// Helper functions para selector de años
function getYearCount() {
  if (!yearFrom.value || !yearTo.value) return 0
  return Math.abs(yearTo.value - yearFrom.value) + 1
}

function getYearRange() {
  if (!yearFrom.value || !yearTo.value) return 'N/A'
  if (yearFrom.value === yearTo.value) return yearFrom.value
  return `${yearFrom.value} a ${yearTo.value}`
}

// Calcular velocidad de procesamiento
function calculateSpeed() {
  if (!execution.value || !execution.value.elapsed_time || execution.value.elapsed_time === 0) {
    return '0'
  }
  const speed = Math.round(execution.value.records_processed / execution.value.elapsed_time)
  return formatNumber(speed)
}

// Calcular tasa de éxito
function calculateSuccessRate() {
  if (!execution.value || !execution.value.records_processed || execution.value.records_processed === 0) {
    return '0'
  }
  const errors = execution.value.records_errors || execution.value.records_failed || 0
  const successful = execution.value.records_processed - errors
  const rate = (successful / execution.value.records_processed) * 100
  return rate.toFixed(1)
}
</script>
