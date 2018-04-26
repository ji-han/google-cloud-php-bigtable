<?php

namespace Google\Cloud\Bigtable\V2;

use \Google\Cloud\Bigtable\V2\BigtableClient;
use \Google\Auth\Credentials\ServiceAccountCredentials;
use \Google\Cloud\Bigtable\V2\Row;
use \Google\Cloud\Bigtable\V2\Family;
use \Google\Cloud\Bigtable\V2\Column;
use \Google\Cloud\Bigtable\V2\Cell;
use \Google\Cloud\Bigtable\V2\RowSet;


class BigtableReadRowsBuffer {
	public $families;
	private $currentRowKey;
	private $currentFamilyName;
	private $currentColumnName;
	private $currentCellTimestampMicros;
	private $stream;
	private $currentValueBuffer;

	public function __construct($stream) {
		$this->reset();
		$this->stream = $stream;
	}

	private function reset() {
		$this->families = [];
		$this->currentRowKey = null;
		$this->currentFamilyName = null;
		$this->currentColumnName = null;
		$this->currentCellTimestampMicros = null;
		$this->currentValueBuffer = '';

	}

	private function flushCell() {
		$colName = $this->currentColumnName;
		$familyName = $this->currentFamilyName;

		if (!array_key_exists($familyName, $this->families)) {
			$this->families[$familyName] = [];
		};

		$family = $this->families[$familyName];

		if (!array_key_exists($colName, $family)) {
			$family[$colName] = [];
		};

		$cell = new Cell();
		$cell->setTimestampMicros($this->currentCellTimestampMicros);
		$cell->setValue($this->currentValueBuffer);

		$this->families[$familyName][$colName][] = $cell;
		$this->currentValueBuffer = '';
	}

	public function yieldRows() {
		foreach ($this->stream->readAll() as $response) {
			foreach ($response->getChunks() as $chunk) {
				if ($chunk->getResetRow()) {
					$this->reset();
					continue;
				};

				$rowKey = $chunk->getRowKey();
				$qualifier = $chunk->getQualifier();
				$familyName = $chunk->getFamilyName();
				$timestampMicros = $chunk->getTimestampMicros();

				if (!is_null($rowKey) && $rowKey != '') {
					$this->currentRowKey = $rowKey;
				}

				if (!is_null($qualifier)) {
					$this->currentColumnName = $qualifier->getValue();
				};

				if (!is_null($familyName)) {
					$this->currentFamilyName = $familyName->getValue();
				};

				if (!is_null($timestampMicros)) {
					$this->currentCellTimestampMicros = $timestampMicros;
				}

				$this->currentValueBuffer .= $chunk->getValue();

				if ($chunk->getValueSize() == 0) {
					$this->flushCell();
				}

				if ($chunk->getCommitRow()) {
					$currentRowKey = $this->currentRowKey;

					yield [
						'colFamilies' => $this->families,
						'rowKey' => $currentRowKey
					];
					$this->reset();
				}
			};
		}
	}
};
