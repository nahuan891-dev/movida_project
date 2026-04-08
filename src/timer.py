"""
Utilitários para medição de performance e tempo de execução
"""

import time
import logging
import psutil
import os
from typing import Dict, Any, Optional, Callable
from functools import wraps
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class PerformanceTimer:
    """Classe para medir performance, tempo de execução e uso de memória"""

    def __init__(self):
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.checkpoints: Dict[str, float] = {}
        self.metrics: Dict[str, Any] = {}
        self.process = psutil.Process(os.getpid())
        self.start_memory = 0.0
        self.peak_memory = 0.0

    def _get_memory_mb(self) -> float:
        """Retorna uso de memória atual em MB"""
        return self.process.memory_info().rss / (1024 * 1024)

    def start(self) -> None:
        """Inicia o timer e monitoramento de memória"""
        self.start_time = time.time()
        self.start_memory = self._get_memory_mb()
        self.peak_memory = self.start_memory
        logger.info(f"⏱️  Timer iniciado (Memória inicial: {self.start_memory:.2f} MB)")

    def checkpoint(self, name: str) -> None:
        """Registra um checkpoint e atualiza pico de memória"""
        if self.start_time is None:
            logger.warning("Timer não iniciado")
            return

        current_mem = self._get_memory_mb()
        if current_mem > self.peak_memory:
            self.peak_memory = current_mem

        self.checkpoints[name] = time.time()
        elapsed = self.checkpoints[name] - self.start_time
        logger.info(f"📍 Checkpoint '{name}': {elapsed:.2f}s | RAM: {current_mem:.2f} MB")

    def stop(self) -> float:
        """Para o timer e retorna tempo total"""
        if self.start_time is None:
            logger.warning("Timer não foi iniciado")
            return 0.0

        self.end_time = time.time()
        current_mem = self._get_memory_mb()
        if current_mem > self.peak_memory:
            self.peak_memory = current_mem

        total_time = self.end_time - self.start_time
        logger.info(f"⏹️  Timer parado. Tempo: {total_time:.2f}s | Pico RAM: {self.peak_memory:.2f} MB")
        return total_time

    def get_elapsed_time(self) -> float:
        """Retorna tempo decorrido desde o início"""
        if self.start_time is None:
            return 0.0
        return time.time() - self.start_time

    def get_checkpoint_times(self) -> Dict[str, float]:
        """Retorna tempos relativos de cada checkpoint"""
        if self.start_time is None:
            return {}

        result = {}
        for name, checkpoint_time in self.checkpoints.items():
            result[name] = checkpoint_time - self.start_time
        return result

    def get_stage_durations(self) -> Dict[str, float]:
        """Calcula duração entre checkpoints consecutivos"""
        if not self.checkpoints:
            return {}

        sorted_checkpoints = sorted(self.checkpoints.items(), key=lambda x: x[1])
        durations = {}

        for i, (name, time_val) in enumerate(sorted_checkpoints):
            if i == 0:
                # Primeiro checkpoint: tempo desde início
                durations[f"inicio → {name}"] = time_val - self.start_time
            else:
                # Checkpoints subsequentes: tempo entre eles
                prev_name, prev_time = sorted_checkpoints[i-1]
                durations[f"{prev_name} → {name}"] = time_val - prev_time

        # Último checkpoint até o fim (se timer parado)
        if self.end_time and sorted_checkpoints:
            last_name, last_time = sorted_checkpoints[-1]
            durations[f"{last_name} → fim"] = self.end_time - last_time

        return durations

    def add_metric(self, key: str, value: Any) -> None:
        """Adiciona uma métrica personalizada"""
        self.metrics[key] = value

    def get_report(self) -> Dict[str, Any]:
        """Gera relatório completo de performance"""
        total_time = self.stop() if self.end_time is None else (self.end_time - self.start_time)

        return {
            'tempo_total': total_time,
            'checkpoints': self.get_checkpoint_times(),
            'duracao_etapas': self.get_stage_durations(),
            'metricas': self.metrics,
            'timestamp_inicio': self.start_time,
            'timestamp_fim': self.end_time
        }

    def log_report(self) -> None:
        """Exibe relatório de performance no log"""
        report = self.get_report()

        logger.info("=" * 60)
        logger.info("📊 RELATÓRIO DE PERFORMANCE")
        logger.info("=" * 60)
        logger.info(f"⏱️  Tempo Total: {report['tempo_total']:.2f}s")
        logger.info("")

        if report['duracao_etapas']:
            logger.info("📈 Duração por Etapa:")
            for etapa, duracao in report['duracao_etapas'].items():
                logger.info(f"   {etapa}: {duracao:.2f}s")
            logger.info("")

        if report['checkpoints']:
            logger.info("📍 Checkpoints:")
            for name, tempo in report['checkpoints'].items():
                logger.info(f"   {name}: {tempo:.2f}s")
            logger.info("")

        if report['metricas']:
            logger.info("📊 Métricas Adicionais:")
            for key, value in report['metricas'].items():
                logger.info(f"   {key}: {value}")
            logger.info("")

        logger.info("=" * 60)


@contextmanager
def time_block(name: str, timer: Optional[PerformanceTimer] = None):
    """
    Context manager para medir tempo de um bloco de código

    Args:
        name: Nome do bloco
        timer: Timer opcional para registrar checkpoint
    """
    start = time.time()
    logger.info(f"▶️  Iniciando: {name}")

    try:
        yield
    finally:
        elapsed = time.time() - start
        logger.info(f"⏸️  Concluído: {name} ({elapsed:.2f}s)")

        if timer:
            timer.add_metric(f"bloco_{name}", elapsed)


def timed_function(func: Callable) -> Callable:
    """
    Decorador para medir tempo de execução de funções

    Args:
        func: Função a ser decorada

    Returns:
        Função decorada com medição de tempo
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        logger.info(f"▶️  Executando: {func.__name__}")

        try:
            result = func(*args, **kwargs)
            elapsed = time.time() - start
            logger.info(f"✅ Concluído: {func.__name__} ({elapsed:.2f}s)")
            return result
        except Exception as e:
            elapsed = time.time() - start
            logger.error(f"❌ Erro em {func.__name__}: {e} ({elapsed:.2f}s)")
            raise

    return wrapper