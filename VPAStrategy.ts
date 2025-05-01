/**

VPAStrategy.ts
Path: C:\AlgoTrader3\server\strategies\VPAStrategy.ts

Implementation of a Volume Price Analysis (VPA) strategy with candlestick pattern recognition,
trend identification, and support/resistance detection.
This strategy analyzes the relationship between price movement, volume, and candlestick patterns
to identify high-probability trading opportunities.

Enhanced version for real-time streaming data processing.
*/

import { AbstractStrategy } from './AbstractStrategy';
import { StrategyState, ConfigValidationResult } from '../interfaces/BaseStrategy';
import { MarketData } from '../models/MarketData';
import { StrategySignal, SignalType, SignalDirection, OrderType } from '../models/StrategySignal';
import { StrategyConfig } from '../models/StrategyConfig';
import { PerformanceMetrics, Trade } from '../models/PerformanceMetrics';
import { v4 as uuidv4 } from 'uuid';
import { identifyPatterns, PatternResult, PatternStrength } from '../utils/CandlestickPatterns';
import {
    identifyTrend,
    TrendDirection,
    TrendStrength,
    TrendResult,
    checkPotentialReversal
} from '../utils/TrendIdentification';
import {
    detectSupportResistance,
    LevelType,
    LevelStrength,
    PriceLevel,
    SupportResistanceResult,
    isPriceAtLevel,
    analyzeBreakout
} from '../utils/SupportResistanceDetection';
import {
    evaluateEntryRules, evaluateExitRules,
    EntryRulesConfig, ExitRulesConfig,
    DEFAULT_ENTRY_RULES_CONFIG, DEFAULT_EXIT_RULES_CONFIG,
    EntryDecision, ExitDecision, VPAAnalysisResult
} from '../utils/EntryExitRules';
import {
    applyEntryConfirmationFilters,
    ConfirmationFiltersConfig,
    DEFAULT_CONFIRMATION_FILTERS_CONFIG
} from '../utils/ConfirmationFilters';
// Import VolumeProfileCalculations
const VolumeProfileCalculations = require('../utils/volume-profile-calculations');
/**

VPA strategy configuration parameters
/
interface VPAStrategyParams {
/*

Minimum volume threshold for significant bars
*/
volumeThreshold: number;

/**

Number of bars to look back for volume analysis
*/
lookbackPeriod: number;

/**

Minimum volume increase to consider a volume surge
*/
volumeSurgeMultiplier: number;

/**

Confidence threshold required to generate signals (0-100)
*/
confidenceThreshold: number;

/**

How significant price should be in the analysis (0-1)
*/
priceWeight: number;

/**

How significant volume should be in the analysis (0-1)
*/
volumeWeight: number;

/**

How significant candlestick patterns should be in the analysis (0-1)
*/
patternWeight: number;

/**

How significant trend should be in the analysis (0-1)
*/
trendWeight: number;

/**

How significant support/resistance should be in the analysis (0-1)
*/
srWeight: number;
// Continued from VPAStrategy Block #1
/**
* Whether to use multi-timeframe analysis
*/
useMultiTimeframe: boolean;
/**
 * Primary timeframe for analysis
 */
primaryTimeframe: string;

/**
 * Higher timeframe for trend confirmation
 */
trendTimeframe: string;

/**
 * Lower timeframe for entry timing
 */
entryTimeframe: string;

/**
 * Entry rules configuration
 */
entryRules: EntryRulesConfig;

/**
 * Exit rules configuration
 */
exitRules: ExitRulesConfig;

/**
 * Confirmation filters configuration
 */
confirmationFilters: ConfirmationFiltersConfig;

/**
 * Real-time processing settings
 */
realTimeSettings: {
    /**
     * Minimum time between signal evaluations in milliseconds
     */
    signalEvaluationInterval: number;

    /**
     * Enable continuous monitoring of positions
     */
    continuousPositionMonitoring: boolean;

    /**
     * Enable dynamic bar reconstruction
     */
    dynamicBarReconstruction: boolean;

    /**
     * Enable order book integration (if available)
     */
    useOrderBookData: boolean;

    /**
     * Maximum number of bars to keep in memory per timeframe
     */
    maxBarsInMemory: number;
};
}
/**

Interface for a bar under construction
/
interface BarUnderConstruction {
/*

Instrument symbol
*/
instrument: string;

/**

Timeframe of the bar
*/
timeframe: string;

/**

Opening timestamp of the bar
*/
openTimestamp: Date;

/**

Expected closing timestamp of the bar
*/
expectedCloseTimestamp: Date;

/**

Opening price
*/
open: number;

/**

Highest price
*/
high: number;

/**

Lowest price
*/
low: number;

/**

Current price (will be closing price when bar completes)
*/
current: number;

/**

Cumulative volume
*/
volume: number;

/**

Number of updates received for this bar
*/
updateCount: number;

/**

Last update timestamp
*/
lastUpdateTime: Date;

/**

Whether this bar has been analyzed in its current state
*/
analyzed: boolean;
}



/**

Interface for tracking active positions
/
interface ActivePositionTracker {
/*

Signal ID
*/
signalId: string;

/**

Trade direction
*/
direction: SignalDirection;

/**

Entry price
*/
entryPrice: number;

/**

Current price
*/
currentPrice: number;

/**

Stop loss price
*/
stopLoss: number | null;

/**

Target price
*/
targetPrice: number | null;

/**

Position quantity
*/
quantity: number;

/**

Entry time
*/
entryTime: Date;

/**

Current P&L
*/
currentPnL: number;

/**

Last time position was checked
*/
lastCheckTime: Date;

/**

Last time a trailing stop was updated
*/
lastTrailingStopUpdate: Date | null;

/**

Highest price reached for long positions or lowest price for shorts
*/
extremePrice: number | null;

/**

Whether trailing stop is active
*/
trailingStopActive: boolean;

/**

Percentage of position taken at each take profit level
*/
takeProfitStatus: {
    level: number;
    price: number;
    portion: number;
    taken: boolean;
} [];
}
/**

Type for bars indexed by timeframe and instrument
*/
type TimeframeInstrumentBars = {
    [timeframe: string]: {
        [instrument: string]: MarketData[];
    };
};

/**

Real-time performance metrics
/
interface RealTimePerformanceData {
/*

Average processing time per tick in milliseconds
*/
averageTickProcessingTimeMs: number;

/**

Number of ticks processed
*/
ticksProcessed: number;

/**

Last update time
*/
lastUpdateTime: Date;
}



/**

Volume Price Analysis (VPA) Strategy with real-time processing capabilities
/
export class VPAStrategy extends AbstractStrategy {
/*

Historical market data cache for analysis
Stored by timeframe and instrument
*/
private _marketDataCache: TimeframeInstrumentBars = {};

/**

Bars currently under construction
Indexed by timeframe and instrument
*/
private _barsUnderConstruction: {
    [timeframe: string]: {
        [instrument: string]: BarUnderConstruction;
    };
} = { };

/**

Ticks collected since last bar update
Used for volume calculation in real-time
*/
private _ticksCollectedSinceLastUpdate: {
    [instrument: string]: {
        count: number;
        volume: number;
        lastPrice: number;
        lastTimestamp: Date;
    };
} = { };

/**

Active signals map
*/
private _activeSignals: Map<string, StrategySignal> = new Map();

/**

Active positions being tracked in real-time
*/
private _activePositionTrackers: Map<string, ActivePositionTracker> = new Map();

/**

Completed trades
*/
private _completedTrades: Trade[] = [];

/**

VPA-specific parameters extracted from configuration
*/
private _vpaParams: VPAStrategyParams | null = null;

/**

Last time signals were evaluated for each instrument
*/
private _lastSignalEvaluationTime: {
    [instrument: string]: Date;
} = { };

/**

Order book depth data if available
*/
private _orderBookData: {
    [instrument: string]: {
        bids: { price: number; volume: number } [];
        asks: { price: number; volume: number } [];
        lastUpdateTime: Date;
    };
} = { };

/**

Volume Profile Calculator component
*/
private _volumeProfileCalculator: any;

/**

Real-time strategy state
/
private _realTimeState: {
/*

Whether the strategy is currently in a position monitoring cycle
*/
isMonitoringPositions: boolean;

/**

Timestamp of last position monitoring cycle
*/
lastPositionMonitoringTime: Date | null;

/**

Timestamp of last bar completion
*/
lastBarCompletionTime: Date | null;

/**

Counter for real-time ticks processed
*/
ticksProcessed: number;

/**

Performance tracking: average processing time per tick in ms
*/
averageTickProcessingTimeMs: number;

/**

Performance tracking: number of ticks used for average calculation
*/
tickProcessingTimeSamples: number;
} = {
    isMonitoringPositions: false,
        lastPositionMonitoringTime: null,
            lastBarCompletionTime: null,
                ticksProcessed: 0,
                    averageTickProcessingTimeMs: 0,
                        tickProcessingTimeSamples: 0
};



/**

Constructor
*/
constructor() {
    super(
        'vpa-strategy-002', // Updated ID to reflect streaming data capability
        'Real-time VPA Strategy',
        '2.0.0', // Updated version for real-time processing
        'Enhanced Volume Price Analysis strategy with real-time streaming data processing capabilities. Identifies high-probability trading opportunities by analyzing the relationship between price movement, volume, candlestick patterns, trend characteristics, and support/resistance levels in real-time.'
    );
}
/**
* Get strategy performance metrics
*/
public async getPerformanceMetrics(): Promise < PerformanceMetrics > {
    // Basic calculation of performance metrics from completed trades
    const now = new Date();
    const startDate = new Date(now);
    startDate.setDate(startDate.getDate() - 30); // Last 30 days
    const trades = this._completedTrades;
    const winningTrades = trades.filter(t => (t.profit || 0) > 0);
    const losingTrades = trades.filter(t => (t.profit || 0) <= 0);

    const grossProfit = winningTrades.reduce((sum, t) => sum + (t.profit || 0), 0);
    const grossLoss = Math.abs(losingTrades.reduce((sum, t) => sum + (t.profit || 0), 0));

    // Note: we're not including realTimePerformance in the returned object
    // since it's not part of the PerformanceMetrics interface
    const metrics: PerformanceMetrics = {
        strategyId: this.id,
        period: {
            start: startDate,
            end: now
        },
        trading: {
            totalTrades: trades.length,
            winningTrades: winningTrades.length,
            losingTrades: losingTrades.length,
            winRate: trades.length > 0 ? winningTrades.length / trades.length * 100 : 0,
            profitFactor: grossLoss > 0 ? grossProfit / grossLoss : grossProfit > 0 ? Infinity : 0,
            averageWin: winningTrades.length > 0 ? grossProfit / winningTrades.length : 0,
            averageLoss: losingTrades.length > 0 ? grossLoss / losingTrades.length : 0,
            winLossRatio: losingTrades.length > 0 && winningTrades.length > 0 ?
                (grossProfit / winningTrades.length) / (grossLoss / losingTrades.length) : 0,
            largestWin: winningTrades.length > 0 ?
                Math.max(...winningTrades.map(t => t.profit || 0)) : 0,
            largestLoss: losingTrades.length > 0 ?
                Math.max(...losingTrades.map(t => Math.abs(t.profit || 0))) : 0,
            maxConsecutiveWins: this._calculateMaxConsecutive(trades, true),
            maxConsecutiveLosses: this._calculateMaxConsecutive(trades, false),
            averageTradeDuration: trades
                .filter(t => t.durationMs !== undefined)
                .length > 0 ?
                trades
                    .filter(t => t.durationMs !== undefined)
                    .reduce((sum, t) => sum + (t.durationMs || 0), 0) /
                trades.filter(t => t.durationMs !== undefined).length :
                0,
            tradesPerDay: trades.length / 30, // Based on last 30 days
            netProfit: trades.reduce((sum, t) => sum + (t.profit || 0), 0),
            grossProfit,
            grossLoss
        },
        risk: {
            maxDrawdownDollars: this._calculateMaxDrawdown(trades),
            maxDrawdownPercent: 0, // Would need account balance history to calculate
            maxDrawdownDuration: 0, // Would need more data to calculate
            recoveryFactor: 0, // Would need more data to calculate
            sharpeRatio: 0, // Would need daily returns to calculate
            sortinoRatio: 0, // Would need daily returns to calculate
            calmarRatio: 0, // Would need more data to calculate
            stdDevReturns: 0, // Would need daily returns to calculate
            downsideDeviation: 0 // Would need daily returns to calculate
        },
        daily: {
            bestDay: 0, // Would need daily aggregation to calculate
            worstDay: 0, // Would need daily aggregation to calculate
            profitableDays: 0, // Would need daily aggregation to calculate
            averageDailyPnL: trades.reduce((sum, t) => sum + (t.profit || 0), 0) / 30
        },
        signalQuality: {
            averageConfidence: 0, // Would need to track signal confidence
            confidenceProfitCorrelation: 0, // Would need to track signal confidence
            averageSignalLatency: 0, // Would need to track signal generation and execution times
            averageSlippage: 0 // Would need to track expected vs. actual execution prices
        },
        recentTrades: this._completedTrades.slice(-10), // Last 10 trades
        calculatedAt: new Date()
    };

    return metrics;
}

/**
 * Submit signal for evaluation
 * @param signal The signal to evaluate
 * @private
 */
private async _submitSignalForEvaluation(signal: StrategySignal): Promise < void> {
    try {
        // Create signals directory if it doesn't exist
        const fs = require('fs').promises;
        const path = require('path');
        const signalDir = path.join(process.cwd(), 'data/signals');

        try {
            await fs.mkdir(signalDir, { recursive: true });
        } catch(err) {
            // Directory already exists or other error
            console.warn(`Error creating signals directory: ${err.message}`);
        }

        // Save signal to file for evaluation
        const signalPath = path.join(signalDir, `${signal.id}.json`);
        await fs.writeFile(signalPath, JSON.stringify(signal, null, 2));

        console.log(`Signal ${signal.id} submitted for quality evaluation`);
    } catch(err) {
        console.error(`Error submitting signal for evaluation: ${err.message}`);
        // Non-critical error, don't throw
    }
}
/**
* Generate an entry signal based on entry decision
* @param data Current market data
* @param analysis VPA analysis result
* @param entryDecision Entry decision
*/
private _generateEntrySignal(
    data: MarketData,
    analysis: VPAAnalysisResult,
    entryDecision: EntryDecision
): StrategySignal {
    // Determine quantity based on risk management settings
    let quantity = entryDecision.suggestedPositionSize;
    // If no suggested size, calculate based on risk management settings
    if (!quantity) {
        quantity = this._calculatePositionSize(
            data.instrument,
            entryDecision.direction,
            entryDecision.entryPrice,
            entryDecision.stopLoss
        );
    }

    // Ensure positive quantity
    if (quantity <= 0) {
        quantity = 1; // Minimum quantity
    }

    // Generate the signal
    const signal: StrategySignal = {
        id: `entry-${uuidv4()}`,
        strategyId: this.id,
        type: SignalType.ENTRY,
        direction: entryDecision.direction,
        instrument: data.instrument,
        quantity,
        orderType: entryDecision.orderType,
        timestamp: new Date(),
        timeInForce: 'DAY',
        confidence: entryDecision.confidence,
        targetPrice: entryDecision.targetPrice || undefined,
        stopLoss: entryDecision.stopLoss || undefined,
        notes: `Real-time VPA Entry Signal - Reasons: ${entryDecision.reasons.join('; ')}`,
        metadata: {
            analysisResult: analysis,
            entryDecision,
            entryPrice: entryDecision.entryPrice,
            isRealTime: true,
            streamingData: true,
            barCompletion: data.isComplete ? 'complete' : 'incomplete'
        }
    };

    // Store the signal
    this._activeSignals.set(signal.id, signal);

    // Submit for evaluation
    this._submitSignalForEvaluation(signal);

    return signal;
}

/**
 * Generate an exit signal based on exit decision
 * @param data Current market data
 * @param position Position to exit
 * @param exitDecision Exit decision
 */
private _generateExitSignal(
    data: MarketData,
    position: StrategySignal,
    exitDecision: ExitDecision
): StrategySignal {
    // Calculate exit quantity (handle partial exits)
    const exitQuantity = Math.max(1, Math.floor(position.quantity * exitDecision.portionToExit));

    // Generate the signal
    const signal: StrategySignal = {
        id: `exit-${uuidv4()}`,
        strategyId: this.id,
        type: SignalType.EXIT,
        direction: position.direction === SignalDirection.LONG ? SignalDirection.SHORT : SignalDirection.LONG,
        instrument: data.instrument,
        quantity: exitQuantity,
        orderType: exitDecision.orderType,
        timestamp: new Date(),
        timeInForce: 'DAY',
        confidence: position.confidence, // Use confidence from the original position
        relatedSignalId: position.id, // Reference to the entry signal
        notes: `Real-time VPA Exit Signal - Type: ${exitDecision.exitType}, Reasons: ${exitDecision.reasons.join('; ')}`,
        metadata: {
            exitDecision,
            exitPrice: exitDecision.exitPrice,
            isRealTime: true,
            streamingData: true,
            barCompletion: data.isComplete ? 'complete' : 'incomplete',
            exitType: exitDecision.exitType
        }
    };

    // Store the signal
    this._activeSignals.set(signal.id, signal);

    // Submit for evaluation
    this._submitSignalForEvaluation(signal);

    return signal;
}

/**
 * Get real-time performance data (separate from standard performance metrics)
 */
public getRealTimePerformanceData(): RealTimePerformanceData {
    return {
        averageTickProcessingTimeMs: this._realTimeState.averageTickProcessingTimeMs,
        ticksProcessed: this._realTimeState.ticksProcessed,
        lastUpdateTime: new Date()
    };
}
/**
* Validate strategy configuration
* @param config Configuration to validate
*/
public async validateConfig(config: Partial<StrategyConfig>): Promise < ConfigValidationResult > {
    const errors: string[] = [];
    const warnings: string[] = [];
    /**
* Strategy-specific initialization logic
* @param config Strategy configuration
*/
    protected async onInitialize(config: StrategyConfig): Promise<boolean> {
        // Extract and store VPA-specific parameters
        const params = config.parameters as any;
        // Default real-time settings if not provided
        const defaultRealTimeSettings = {
            signalEvaluationInterval: 1000, // 1 second
            continuousPositionMonitoring: true,
            dynamicBarReconstruction: true,
            useOrderBookData: false,
            maxBarsInMemory: 500
        };

        this._vpaParams = {
            volumeThreshold: params.volumeThreshold,
            lookbackPeriod: params.lookbackPeriod,
            volumeSurgeMultiplier: params.volumeSurgeMultiplier,
            confidenceThreshold: params.confidenceThreshold,
            priceWeight: params.priceWeight,
            volumeWeight: params.volumeWeight,
            patternWeight: params.patternWeight || 0.2, // Default to 20% weight if not specified
            trendWeight: params.trendWeight || 0.2, // Default to 20% weight if not specified
            srWeight: params.srWeight || 0.2, // Default to 20% weight if not specified
            useMultiTimeframe: params.useMultiTimeframe || false,
            primaryTimeframe: params.primaryTimeframe,
            trendTimeframe: params.trendTimeframe,
            entryTimeframe: params.entryTimeframe,
            entryRules: params.entryRules || DEFAULT_ENTRY_RULES_CONFIG,
            exitRules: params.exitRules || DEFAULT_EXIT_RULES_CONFIG,
            confirmationFilters: params.confirmationFilters || DEFAULT_CONFIRMATION_FILTERS_CONFIG,
            realTimeSettings: params.realTimeSettings || defaultRealTimeSettings
        };

        // Initialize Volume Profile Calculator component
        this._volumeProfileCalculator = new VolumeProfileCalculations({
            valueAreaPercent: 70,
            highVolumeNodeThreshold: 1.5,
            volumeSpikeThreshold: 2.0,
            // Other configuration options can be added as needed
        });

        // Initialize data structures for each configured instrument and timeframe
        const timeframes = this._getTimeframes();
        for (const timeframe of timeframes) {
            this._marketDataCache[timeframe] = {};
            this._barsUnderConstruction[timeframe] = {};

            for (const instrument of config.instruments) {
                this._marketDataCache[timeframe][instrument] = [];
                this._barsUnderConstruction[timeframe][instrument] = null;
                this._ticksCollectedSinceLastUpdate[instrument] = {
                    count: 0,
                    volume: 0,
                    lastPrice: 0,
                    lastTimestamp: new Date()
                };
                this._lastSignalEvaluationTime[instrument] = new Date(0); // Initialize to epoch
                this._orderBookData[instrument] = {
                    bids: [],
                    asks: [],
                    lastUpdateTime: new Date(0)
                };
            }
        }

        // Initialize real-time state
        this._realTimeState = {
            isMonitoringPositions: false,
            lastPositionMonitoringTime: null,
            lastBarCompletionTime: null,
            ticksProcessed: 0,
            averageTickProcessingTimeMs: 0,
            tickProcessingTimeSamples: 0
        };

        return true;
    }

/**
 * Analyze volume profile for the instrument and timeframe
 * @param instrument Instrument to analyze
 * @param startDate Analysis start date
 * @param endDate Analysis end date
 * @param timeframe Timeframe to analyze
 * @returns Volume profile analysis results
 */
private async _analyzeVolumeProfile(
        instrument: string,
        startDate: Date,
        endDate: Date,
        timeframe: string
    ): Promise<any> {
        try {
            // Get volume profile via component
            const volumeProfile = await this._volumeProfileCalculator.calculateVolumeProfile({
                instrument,
                period: 'custom', // Or appropriate period
                startDate,
                endDate,
                source: 'file', // Use 'mongo' if MongoDB data is preferred
                session: null // Or specify session if needed
            });

            return volumeProfile;
        } catch (err) {
            console.error(`Error analyzing volume profile: ${err.message}`);
            return null;
        }
    }

/**
 * Check if price is within the value area
 * @param price Current price
 * @param volumeProfile Volume profile data
 * @returns Boolean indicating if price is in value area
 */
private _isPriceInValueArea(price: number, volumeProfile: any): boolean {
        return price >= volumeProfile.valueArea.low &&
            price <= volumeProfile.valueArea.high;
    }

/**
 * Calculate distance from current price to Point of Control
 * @param price Current price
 * @param volumeProfile Volume profile data
 * @returns Distance as percentage
 */
private _calculateDistanceToPOC(price: number, volumeProfile: any): number {
        const poc = volumeProfile.pointOfControl;
        return Math.abs((price - poc) / poc) * 100;
    }

/**
 * Check if price is near a high volume node
 * @param price Current price
 * @param volumeProfile Volume profile data
 * @returns Boolean indicating if near high volume node
 */
private _isNearHighVolumeNode(price: number, volumeProfile: any): boolean {
        // Consider price near a node if within 0.5% of any high volume node
        const threshold = price * 0.005;
        return volumeProfile.highVolumeNodes.some(
            (node: any) => Math.abs(price - node.price) < threshold
        );
}
    /**
* Adjust confidence based on volume profile
* @param baseConfidence Base confidence from other factors
* @param data Current market data
* @param volumeProfile Volume profile data
* @returns Adjusted confidence value
*/
private _adjustConfidenceWithVolumeProfile(
    baseConfidence: number,
    data: MarketData,
    volumeProfile: any
): number {
    let adjustment = 0;
        // Adjust based on whether price is in value area
        if (this._isPriceInValueArea(data.close, volumeProfile)) {
            adjustment += 5; // Higher confidence in value area
        }

        // Adjust based on proximity to POC
        const distanceToPOC = this._calculateDistanceToPOC(data.close, volumeProfile);
        if (distanceToPOC < 0.5) {
            adjustment += 10; // Very close to POC
        } else if (distanceToPOC < 1) {
            adjustment += 5; // Close to POC
        }

        // Adjust based on proximity to high volume nodes
        const isNearHighVolumeNode = this._isNearHighVolumeNode(data.close, volumeProfile);
        if (isNearHighVolumeNode) {
            adjustment += 8; // Near high volume node
        }

        // Cap final confidence at 100
        return Math.min(100, baseConfidence + adjustment);
    }

/**
 * Enhance VPA analysis with volume profile data
 * @param data Current market data
 * @param analysisResult Base VPA analysis result
 * @returns Enhanced analysis with volume profile
 */
private async _enhanceAnalysisWithVolumeProfile(
        data: MarketData,
        analysisResult: VPAAnalysisResult
    ): Promise<VPAAnalysisResult> {
        // Get date range based on lookback period
        const endDate = new Date(data.timestamp);
        const startDate = new Date(endDate);
        startDate.setDate(startDate.getDate() - this._vpaParams.lookbackPeriod);

        // Get volume profile
        const volumeProfile = await this._analyzeVolumeProfile(
            data.instrument,
            startDate,
            endDate,
            data.timeframe
        );

        if (!volumeProfile) {
            return analysisResult; // Return original if no volume profile
        }

        // Enhance analysis with volume profile data
        return {
            ...analysisResult,
            volumeProfile: {
                pointOfControl: volumeProfile.pointOfControl,
                valueAreaHigh: volumeProfile.valueArea.high,
                valueAreaLow: volumeProfile.valueArea.low,
                vwap: volumeProfile.vwap,
                highVolumeNodes: volumeProfile.highVolumeNodes,
                isInValueArea: this._isPriceInValueArea(data.close, volumeProfile),
                distanceToPointOfControl: this._calculateDistanceToPOC(data.close, volumeProfile)
            },
            // Adjust confidence based on volume profile
            confidence: this._adjustConfidenceWithVolumeProfile(
                analysisResult.confidence,
                data,
                volumeProfile
            )
        };
    }

/**
 * Strategy-specific data processing logic for streaming data
 * @param data Market data to process
 */
protected async onProcessData(data: MarketData): Promise<StrategySignal | null> {
        const processingStartTime = Date.now();

        try {
            // Skip if strategy is not properly initialized
            if (!this._vpaParams || !this._config) {
                return null;
            }

            // Increment tick counter for performance tracking
            this._realTimeState.ticksProcessed++;

            // Process real-time data differently based on whether it's a complete bar or a tick update
            if (data.isComplete) {
                // This is a complete bar, process it normally
                return await this._processCompleteBar(data);
            } else {
                // This is a real-time tick update, integrate it into our bar reconstruction
                return await this._processTickUpdate(data);
            }
        } finally {
            // Track processing time for performance metrics
            const processingTime = Date.now() - processingStartTime;
            this._updateProcessingTimeMetrics(processingTime);
        }
    }

/**
 * Process a complete price bar
 * @param data Complete bar data
 */
private async _processCompleteBar(data: MarketData): Promise<StrategySignal | null> {
        // Store the complete bar in the cache
        this._updateMarketDataCache(data);

        // Mark the bar as complete in the under-construction cache
        this._completeBarUnderConstruction(data);

        // Update last bar completion time
        this._realTimeState.lastBarCompletionTime = new Date();

        // Get the relevant data series for analysis
        const dataForAnalysis = this._getDataForAnalysis(data.instrument, data.timeframe);
        if (!dataForAnalysis || dataForAnalysis.length < this._vpaParams.lookbackPeriod) {
            return null; // Not enough data for analysis
        }

        // Check if we need to evaluate signals based on timing interval
        if (!this._shouldEvaluateSignals(data.instrument)) {
            return null;
        }

        // Update signal evaluation time
        this._lastSignalEvaluationTime[data.instrument] = new Date();

        // Perform standard signal generation (similar to original logic)
        const signal = await this._generateSignalsFromAnalysis(data, dataForAnalysis);

        return signal;
}
    /**
* Process a real-time tick update
* @param data Tick update data
*/
private async _processTickUpdate(data: MarketData): Promise<StrategySignal | null> {
        // Update tick statistics
        this._updateTickStats(data);
        // Update or create bar under construction
        this._updateBarUnderConstruction(data);

        // Monitor active positions if enabled
        if (this._vpaParams.realTimeSettings.continuousPositionMonitoring) {
            await this._monitorActivePositions(data);
        }

        // Check if we need to evaluate signals based on timing interval
        // For tick updates, we're more conservative to avoid excessive signal evaluation
        if (!this._shouldEvaluateSignals(data.instrument, true)) {
            return null;
        }

        // Update signal evaluation time
        this._lastSignalEvaluationTime[data.instrument] = new Date();

        // For real-time updates, we'll check for significant market events that might
        // warrant immediate action, even if the bar is incomplete
        const urgentSignal = await this._checkForUrgentSignals(data);
        if (urgentSignal) {
            return urgentSignal;
        }

        return null;
    }

/**
 * Generate signals from market analysis
 * @param data Current market data
 * @param dataForAnalysis Historical data for analysis
 */
private async _generateSignalsFromAnalysis(
        data: MarketData,
        dataForAnalysis: MarketData[]
    ): Promise<StrategySignal | null> {
        // Perform VPA analysis
        let analysisResult = this._performVPAAnalysis(dataForAnalysis);

        // Enhance analysis with volume profile data
        analysisResult = await this._enhanceAnalysisWithVolumeProfile(data, analysisResult);

        // Get higher and lower timeframe analyses if using multi-timeframe mode
        let higherTimeframeAnalysis: VPAAnalysisResult | undefined;
        let lowerTimeframeAnalysis: VPAAnalysisResult | undefined;

        if (this._vpaParams.useMultiTimeframe) {
            // Get higher timeframe data
            if (this._vpaParams.trendTimeframe) {
                const higherTfData = this._getDataForAnalysis(data.instrument, this._vpaParams.trendTimeframe);
                if (higherTfData && higherTfData.length >= this._vpaParams.lookbackPeriod) {
                    higherTimeframeAnalysis = this._performVPAAnalysis(higherTfData);
                    // Enhance with volume profile if available
                    if (higherTimeframeAnalysis) {
                        higherTimeframeAnalysis = await this._enhanceAnalysisWithVolumeProfile(
                            higherTfData[higherTfData.length - 1],
                            higherTimeframeAnalysis
                        );
                    }
                }
            }

            // Get lower timeframe data
            if (this._vpaParams.entryTimeframe) {
                const lowerTfData = this._getDataForAnalysis(data.instrument, this._vpaParams.entryTimeframe);
                if (lowerTfData && lowerTfData.length >= this._vpaParams.lookbackPeriod) {
                    lowerTimeframeAnalysis = this._performVPAAnalysis(lowerTfData);
                    // Enhance with volume profile if available
                    if (lowerTimeframeAnalysis) {
                        lowerTimeframeAnalysis = await this._enhanceAnalysisWithVolumeProfile(
                            lowerTfData[lowerTfData.length - 1],
                            lowerTimeframeAnalysis
                        );
                    }
                }
            }
        }

        // Check if we have any active positions for this instrument
        const activePositions = this._getActivePositions(data.instrument);

        // If we have active positions, evaluate exit rules
        if (activePositions.length > 0) {
            // Evaluate exit rules for each position
            for (const position of activePositions) {
                const exitDecision = evaluateExitRules(
                    {
                        direction: position.direction,
                        entryPrice: position.metadata?.entryPrice || data.close, // Fallback if metadata not available
                        stopLoss: position.stopLoss || null,
                        targetPrice: position.targetPrice || null,
                        entryTime: position.timestamp,
                        currentProfit: this._calculateCurrentProfit(
                            position.direction,
                            position.metadata?.entryPrice || data.close,
                            data.close,
                            position.quantity
                        )
                    },
                    data,
                    dataForAnalysis.slice(0, -1), // Previous bars
                    analysisResult,
                    this._vpaParams.exitRules
                );

                // Generate exit signal if needed
                if (exitDecision.shouldExit) {
                    return this._generateExitSignal(data, position, exitDecision);
                }
            }
        }

        // If no active positions or no exits needed, evaluate entry rules
        const entryDecision = evaluateEntryRules(
            data,
            dataForAnalysis.slice(0, -1), // Previous bars
            analysisResult,
            this._vpaParams.entryRules
        );

        // Apply confirmation filters to entry decision
        const filteredDecision = applyEntryConfirmationFilters(
            entryDecision,
            data,
            dataForAnalysis.slice(0, -1),
            analysisResult,
            this._vpaParams.confirmationFilters
        );

        // Generate entry signal if conditions are met (use filtered decision)
        if (filteredDecision.shouldEnter) {
            return this._generateEntrySignal(data, analysisResult, filteredDecision);
        }

        return null;
}
    **
* Check for urgent signals in real - time that can't wait for bar completion
    * @param data Tick update data
        * /
private async _checkForUrgentSignals(data: MarketData): Promise < StrategySignal | null > {
    // Only check if we have active positions
    const activePositions = this._getActivePositions(data.instrument);
    if(activePositions.length === 0) {
    return null;
}
// For each position, check if we need to take urgent action
for (const position of activePositions) {
    // Get position tracker if exists
    const positionTracker = this._activePositionTrackers.get(position.id);
    if (!positionTracker) {
        continue;
    }

    // Check for stop loss triggers
    if (position.stopLoss !== undefined && position.stopLoss !== null) {
        const currentPrice = data.close;

        if ((position.direction === SignalDirection.LONG && currentPrice <= position.stopLoss) ||
            (position.direction === SignalDirection.SHORT && currentPrice >= position.stopLoss)) {

            // Create an emergency exit decision
            const exitDecision: ExitDecision = {
                shouldExit: true,
                exitType: 'stop_loss',
                reasons: ['Stop loss triggered in real-time'],
                exitPrice: currentPrice,
                portionToExit: 1.0,
                orderType: OrderType.MARKET
            };

            // Generate exit signal with high urgency flag
            const exitSignal = this._generateExitSignal(data, position, exitDecision);

            // Add metadata to indicate this was an urgent real-time exit
            exitSignal.metadata = {
                ...exitSignal.metadata,
                urgentRealTimeExit: true,
                triggerType: 'stop_loss',
                priceAtTrigger: currentPrice
            };

            return exitSignal;
        }
    }

    // Check for take profit triggers
    if (position.targetPrice !== undefined && position.targetPrice !== null) {
        const currentPrice = data.close;

        if ((position.direction === SignalDirection.LONG && currentPrice >= position.targetPrice) ||
            (position.direction === SignalDirection.SHORT && currentPrice <= position.targetPrice)) {

            // Create an emergency exit decision for take profit
            const exitDecision: ExitDecision = {
                shouldExit: true,
                exitType: 'take_profit',
                reasons: ['Take profit triggered in real-time'],
                exitPrice: currentPrice,
                portionToExit: 1.0, // Full exit for simplicity
                orderType: OrderType.MARKET
            };

            // Generate exit signal with high urgency flag
            const exitSignal = this._generateExitSignal(data, position, exitDecision);

            // Add metadata to indicate this was an urgent real-time exit
            exitSignal.metadata = {
                ...exitSignal.metadata,
                urgentRealTimeExit: true,
                triggerType: 'take_profit',
                priceAtTrigger: currentPrice
            };

            return exitSignal;
        }
    }

    // Check for trailing stop triggers if active
    if (positionTracker.trailingStopActive && positionTracker.extremePrice !== null) {
        const currentPrice = data.close;
        const trailingDistance = this._vpaParams.exitRules.trailingStopDistancePercent / 100;

        let trailingStopLevel = null;

        if (position.direction === SignalDirection.LONG) {
            trailingStopLevel = positionTracker.extremePrice * (1 - trailingDistance);

            if (currentPrice <= trailingStopLevel) {
                // Trailing stop triggered for long position
                const exitDecision: ExitDecision = {
                    shouldExit: true,
                    exitType: 'trailing_stop',
                    reasons: ['Trailing stop triggered in real-time'],
                    exitPrice: currentPrice,
                    portionToExit: 1.0,
                    orderType: OrderType.MARKET
                };

                return this._generateExitSignal(data, position, exitDecision);
            }
        } else if (position.direction === SignalDirection.SHORT) {
            trailingStopLevel = positionTracker.extremePrice * (1 + trailingDistance);

            if (currentPrice >= trailingStopLevel) {
                // Trailing stop triggered for short position
                const exitDecision: ExitDecision = {
                    shouldExit: true,
                    exitType: 'trailing_stop',
                    reasons: ['Trailing stop triggered in real-time'],
                    exitPrice: currentPrice,
                    portionToExit: 1.0,
                    orderType: OrderType.MARKET
                };

                return this._generateExitSignal(data, position, exitDecision);
            }
        }
    }

    // Check for partial take profit levels
    if (positionTracker.takeProfitStatus && positionTracker.takeProfitStatus.length > 0) {
        const currentPrice = data.close;

        for (let i = 0; i < positionTracker.takeProfitStatus.length; i++) {
            const tpLevel = positionTracker.takeProfitStatus[i];

            if (!tpLevel.taken && (
                (position.direction === SignalDirection.LONG && currentPrice >= tpLevel.price) ||
                (position.direction === SignalDirection.SHORT && currentPrice <= tpLevel.price)
            )) {
                // Take profit level triggered
                const exitDecision: ExitDecision = {
                    shouldExit: true,
                    exitType: 'take_profit', // Changed from 'partial_take_profit' to match allowed exitType values
                    reasons: [`Partial take profit level ${i + 1} triggered in real-time`],
                    exitPrice: currentPrice,
                    portionToExit: tpLevel.portion,
                    orderType: OrderType.MARKET
                };

                // Mark this level as taken
                positionTracker.takeProfitStatus[i].taken = true;

                return this._generateExitSignal(data, position, exitDecision);
            }
        }
    }
}

return null;
}
/**
* Monitor active positions for real-time updates
* @param data Current market data
*/
private async _monitorActivePositions(data: MarketData): Promise < void> {
    // Skip if no active positions or already monitoring
    if(this._realTimeState.isMonitoringPositions) {
    return;
}
// Set monitoring flag
this._realTimeState.isMonitoringPositions = true;

try {
    // Get all active positions
    const activePositions = Array.from(this._activeSignals.values())
        .filter(signal => signal.type === SignalType.ENTRY &&
            signal.instrument === data.instrument);

    // Update position trackers
    for (const position of activePositions) {
        // Get or create position tracker
        let tracker = this._activePositionTrackers.get(position.id);

        if (!tracker) {
            // Create new tracker
            tracker = {
                signalId: position.id,
                direction: position.direction,
                entryPrice: position.metadata?.entryPrice || data.close,
                currentPrice: data.close,
                stopLoss: position.stopLoss || null,
                targetPrice: position.targetPrice || null,
                quantity: position.quantity,
                entryTime: position.timestamp,
                currentPnL: this._calculateCurrentProfit(
                    position.direction,
                    position.metadata?.entryPrice || data.close,
                    data.close,
                    position.quantity
                ),
                lastCheckTime: new Date(),
                lastTrailingStopUpdate: null,
                extremePrice: data.close,
                trailingStopActive: false,
                takeProfitStatus: []
            };

            // Initialize take profit levels if configured
            if (this._vpaParams.exitRules.useTakeProfits &&
                this._vpaParams.exitRules.takeProfitLevels &&
                this._vpaParams.exitRules.takeProfitLevels.length > 0) {

                for (const tpLevel of this._vpaParams.exitRules.takeProfitLevels) {
                    const level = tpLevel.percent / 100;
                    let price: number;

                    if (position.direction === SignalDirection.LONG) {
                        price = tracker.entryPrice * (1 + level);
                    } else {
                        price = tracker.entryPrice * (1 - level);
                    }

                    tracker.takeProfitStatus.push({
                        level: tpLevel.percent,
                        price,
                        portion: tpLevel.positionPortion,
                        taken: false
                    });
                }
            }

            this._activePositionTrackers.set(position.id, tracker);
        } else {
            // Update existing tracker
            tracker.currentPrice = data.close;
            tracker.currentPnL = this._calculateCurrentProfit(
                tracker.direction,
                tracker.entryPrice,
                data.close,
                tracker.quantity
            );
            tracker.lastCheckTime = new Date();

            // Update extreme price for trailing stop
            if (tracker.direction === SignalDirection.LONG) {
                if (tracker.extremePrice === null || data.close > tracker.extremePrice) {
                    tracker.extremePrice = data.close;
                    tracker.lastTrailingStopUpdate = new Date();
                }
            } else {
                if (tracker.extremePrice === null || data.close < tracker.extremePrice) {
                    tracker.extremePrice = data.close;
                    tracker.lastTrailingStopUpdate = new Date();
                }
            }

            // Check if trailing stop should be activated
            if (!tracker.trailingStopActive &&
                this._vpaParams.exitRules.useTrailingStops &&
                tracker.extremePrice !== null) {

                const activationPercent = this._vpaParams.exitRules.trailingStopActivationPercent / 100;

                if (tracker.direction === SignalDirection.LONG) {
                    const requiredPrice = tracker.entryPrice * (1 + activationPercent);
                    if (tracker.extremePrice >= requiredPrice) {
                        tracker.trailingStopActive = true;
                    }
                } else {
                    const requiredPrice = tracker.entryPrice * (1 - activationPercent);
                    if (tracker.extremePrice <= requiredPrice) {
                        tracker.trailingStopActive = true;
                    }
                }
            }
        }
    }

    // Clean up position trackers for closed positions
    const activePositionIds = new Set(activePositions.map(p => p.id));
    const trackersToRemove: string[] = [];

    for (const [id, tracker] of this._activePositionTrackers.entries()) {
        if (!activePositionIds.has(tracker.signalId)) {
            trackersToRemove.push(id);
        }
    }

    for (const id of trackersToRemove) {
        this._activePositionTrackers.delete(id);
    }

    // Update monitoring timestamp
    this._realTimeState.lastPositionMonitoringTime = new Date();
} finally {
    // Reset monitoring flag
    this._realTimeState.isMonitoringPositions = false;
}
}

// Preserve existing real-time settings if not provided in new config
    const currentRealTimeSettings = this._vpaParams?.realTimeSettings || {
    signalEvaluationInterval: 1000,
    continuousPositionMonitoring: true,
    dynamicBarReconstruction: true,
    useOrderBookData: false,
    maxBarsInMemory: 500
};

const newRealTimeSettings = params.realTimeSettings || currentRealTimeSettings;

this._vpaParams = {
    volumeThreshold: params.volumeThreshold,
    lookbackPeriod: params.lookbackPeriod,
    volumeSurgeMultiplier: params.volumeSurgeMultiplier,
    confidenceThreshold: params.confidenceThreshold,
    priceWeight: params.priceWeight,
    volumeWeight: params.volumeWeight,
    patternWeight: params.patternWeight || 0.2,
    trendWeight: params.trendWeight || 0.2,
    srWeight: params.srWeight || 0.2,
    useMultiTimeframe:
    // Preserve existing real-time settings if not provided in new config
    const currentRealTimeSettings = this._vpaParams?.realTimeSettings || {
        signalEvaluationInterval: 1000,
        continuousPositionMonitoring: true,
        dynamicBarReconstruction: true,
        useOrderBookData: false,
        maxBarsInMemory: 500
    };

    const newRealTimeSettings = params.realTimeSettings || currentRealTimeSettings;

    this._vpaParams = {
        volumeThreshold: params.volumeThreshold,
        lookbackPeriod: params.lookbackPeriod,
        volumeSurgeMultiplier: params.volumeSurgeMultiplier,
        confidenceThreshold: params.confidenceThreshold,
        priceWeight: params.priceWeight,
        volumeWeight: params.volumeWeight,
        patternWeight: params.patternWeight || 0.2,
        trendWeight: params.trendWeight || 0.2,
        srWeight: params.srWeight || 0.2,
        useMultiTimeframe: params.useMultiTimeframe || false,
        primaryTimeframe: params.primaryTimeframe,
        trendTimeframe: params.trendTimeframe,
        entryTimeframe: params.entryTimeframe,
        entryRules: params.entryRules || DEFAULT_ENTRY_RULES_CONFIG,
        exitRules: params.exitRules || DEFAULT_EXIT_RULES_CONFIG,
        confirmationFilters: params.confirmationFilters || DEFAULT_CONFIRMATION_FILTERS_CONFIG,
        realTimeSettings: newRealTimeSettings
    };

    // Check if instruments have changed
    const oldInstruments = new Set(oldConfig.instruments);
    const newInstruments = new Set(newConfig.instruments);

    // Initialize data structures for new instruments
    const timeframes = this._getTimeframes();
    for(const timeframe of timeframes) {
        if (!this._marketDataCache[timeframe]) {
            this._marketDataCache[timeframe] = {};
        }

        if (!this._barsUnderConstruction[timeframe]) {
            this._barsUnderConstruction[timeframe] = {};
        }

        for (const instrument of newConfig.instruments) {
            if (!oldInstruments.has(instrument)) {
                this._marketDataCache[timeframe][instrument] = [];
                this._barsUnderConstruction[timeframe][instrument] = null;
                this._ticksCollectedSinceLastUpdate[instrument] = {
                    count: 0,
                    volume: 0,
                    lastPrice: 0,
                    lastTimestamp: new Date()
                };
                this._lastSignalEvaluationTime[instrument] = new Date(0);
                this._orderBookData[instrument] = {
                    bids: [],
                    asks: [],
                    lastUpdateTime: new Date(0)
                };
            }
        }
    }

    return true;
}

/**
 * Strategy-specific shutdown logic
 * @param fromState State before shutdown was initiated
 */
protected async onShutdown(fromState: StrategyState): Promise < boolean > {
    // Clear data caches to free memory
    this._marketDataCache = {};
    this._barsUnderConstruction = {};
    this._ticksCollectedSinceLastUpdate = {};
    this._activePositionTrackers.clear();

    return true;
}
/**
* Strategy-specific execution feedback logic
* @param signalId ID of the signal that was executed
* @param executionData Data about how the signal was executed
*/
protected async onExecutionFeedback(signalId: string, executionData: any): Promise < boolean > {
    // Find the signal
    const signal = this._activeSignals.get(signalId);
    if(!signal) {
        return false;
    }
// Handle different feedback types
    if(executionData.status === 'filled') {
    // For entry signals, track the position
    if (signal.type === SignalType.ENTRY) {
        // Create trade record
        const trade: Trade = {
            id: uuidv4(),
            strategyId: this.id,
            signalId: signal.id,
            instrument: signal.instrument,
            direction: signal.direction,
            entryTime: executionData.fillTime ? new Date(executionData.fillTime) : new Date(),
            entryPrice: executionData.fillPrice,
            quantity: executionData.fillQuantity || signal.quantity,
            tags: ['vpa-strategy', 'real-time'],
            notes: signal.notes
        };

        // Store for later completion
        this._activeSignals.set(`trade-${trade.id}`, {
            ...signal,
            id: `trade-${trade.id}`,
            relatedSignalId: signalId,
            metadata: {
                ...signal.metadata,
                entryPrice: executionData.fillPrice,
                entryTime: executionData.fillTime ? new Date(executionData.fillTime) : new Date()
            }
        });

        // Create position tracker for real-time monitoring
        const newTracker: ActivePositionTracker = {
            signalId: signal.id,
            direction: signal.direction,
            entryPrice: executionData.fillPrice,
            currentPrice: executionData.fillPrice,
            stopLoss: signal.stopLoss || null,
            targetPrice: signal.targetPrice || null,
            quantity: executionData.fillQuantity || signal.quantity,
            entryTime: executionData.fillTime ? new Date(executionData.fillTime) : new Date(),
            currentPnL: 0, // Starting P&L is 0
            lastCheckTime: new Date(),
            lastTrailingStopUpdate: null,
            extremePrice: executionData.fillPrice,
            trailingStopActive: false,
            takeProfitStatus: []
        };

        // Initialize take profit levels if configured
        if (this._vpaParams.exitRules.useTakeProfits &&
            this._vpaParams.exitRules.takeProfitLevels &&
            this._vpaParams.exitRules.takeProfitLevels.length > 0) {

            for (const tpLevel of this._vpaParams.exitRules.takeProfitLevels) {
                const level = tpLevel.percent / 100;
                let price: number;

                if (signal.direction === SignalDirection.LONG) {
                    price = newTracker.entryPrice * (1 + level);
                } else {
                    price = newTracker.entryPrice * (1 - level);
                }

                newTracker.takeProfitStatus.push({
                    level: tpLevel.percent,
                    price,
                    portion: tpLevel.positionPortion,
                    taken: false
                });
            }
        }

        this._activePositionTrackers.set(signal.id, newTracker);
    }

    // For exit signals, complete the trade
    if (signal.type === SignalType.EXIT && signal.relatedSignalId) {
        const entrySignal = this._activeSignals.get(signal.relatedSignalId);
        if (entrySignal) {
            // Find the trade
            const tradeSignalId = `trade-${signal.relatedSignalId.replace('entry-', '')}`;
            const tradeSignal = this._activeSignals.get(tradeSignalId);

            if (tradeSignal) {
                // Get the trade ID from the tradeSignal
                const tradeId = tradeSignalId.replace('trade-', '');

                // Get the entry price from metadata or fallback to estimate
                const entryPrice = tradeSignal.metadata?.entryPrice || 0;

                // Calculate trade duration if we have entry time
                let durationMs;
                if (tradeSignal.metadata?.entryTime) {
                    const entryTime = tradeSignal.metadata.entryTime;
                    const exitTime = executionData.fillTime ? new Date(executionData.fillTime) : new Date();
                    durationMs = exitTime.getTime() - (entryTime instanceof Date ? entryTime.getTime() : new Date(entryTime).getTime());
                }

                // Complete the trade
                const trade: Trade = {
                    id: tradeId,
                    strategyId: this.id,
                    signalId: entrySignal.id,
                    instrument: entrySignal.instrument,
                    direction: entrySignal.direction,
                    entryTime: tradeSignal.metadata?.entryTime || new Date(),
                    entryPrice: entryPrice,
                    quantity: entrySignal.quantity,
                    exitTime: executionData.fillTime ? new Date(executionData.fillTime) : new Date(),
                    exitPrice: executionData.fillPrice,
                    profit: this._calculateProfit(
                        entrySignal.direction,
                        entryPrice,
                        executionData.fillPrice,
                        entrySignal.quantity
                    ),
                    exitReason: signal.notes || 'Signal exit',
                    durationMs: durationMs,
                    tags: ['vpa-strategy', 'real-time'],
                    notes: signal.notes
                };

                // Store completed trade
                this._completedTrades.push(trade);

                // Clean up signals
                this._activeSignals.delete(signal.id);
                this._activeSignals.delete(entrySignal.id);
                this._activeSignals.delete(tradeSignalId);

                // Remove position tracker
                this._activePositionTrackers.delete(entrySignal.id);
            }
        }
    }

    return true;
}

if (executionData.status === 'rejected' || executionData.status === 'canceled') {
    // Remove the signal from active signals
    this._activeSignals.delete(signalId);
    return true;
}

return true;
}
/**
* Strategy-specific reset logic
* @param fromState State before reset was initiated
*/
protected async onReset(fromState: StrategyState): Promise < boolean > {
    // Clear data caches but maintain the structure
    const timeframes = this._getTimeframes();
    const instruments = this._config?.instruments || [];
    this._marketDataCache = {};
    this._barsUnderConstruction = {};
    this._ticksCollectedSinceLastUpdate = {};

    for(const timeframe of timeframes) {
        this._marketDataCache[timeframe] = {};
        this._barsUnderConstruction[timeframe] = {};

        for (const instrument of instruments) {
            this._marketDataCache[timeframe][instrument] = [];
            this._barsUnderConstruction[timeframe][instrument] = null;
            this._ticksCollectedSinceLastUpdate[instrument] = {
                count: 0,
                volume: 0,
                lastPrice: 0,
                lastTimestamp: new Date()
            };
            this._lastSignalEvaluationTime[instrument] = new Date(0);
        }
    }

    // Clear active signals, position trackers, and trades
    this._activeSignals.clear();
    this._activePositionTrackers.clear();
    this._completedTrades = [];

    // Reset real-time state
    this._realTimeState = {
        isMonitoringPositions: false,
        lastPositionMonitoringTime: null,
        lastBarCompletionTime: null,
        ticksProcessed: 0,
        averageTickProcessingTimeMs: 0,
        tickProcessingTimeSamples: 0
    };

    return true;
}

/**
 * Update the market data cache with new data
 * @param data New market data
 */
private _updateMarketDataCache(data: MarketData): void {
    if(!this._marketDataCache[data.timeframe]) {
    this._marketDataCache[data.timeframe] = {};
}

if (!this._marketDataCache[data.timeframe][data.instrument]) {
    this._marketDataCache[data.timeframe][data.instrument] = [];
}

// Update or add the data
const dataCache = this._marketDataCache[data.timeframe][data.instrument];

// If the data is an update to the current bar, replace the last entry
if (!data.isComplete && dataCache.length > 0) {
    const lastData = dataCache[dataCache.length - 1];
    if (lastData.timestamp.getTime() === data.timestamp.getTime()) {
        dataCache[dataCache.length - 1] = data;
        return;
    }
}

// Add the data to the cache
dataCache.push(data);

// Trim the cache to maximum allowed size
const maxCacheSize = this._vpaParams?.realTimeSettings?.maxBarsInMemory || 500;
if (dataCache.length > maxCacheSize) {
    this._marketDataCache[data.timeframe][data.instrument] = dataCache.slice(-maxCacheSize);
}
}

/**
 * Update tick statistics for instrument
 * @param data Current tick data
 */
private _updateTickStats(data: MarketData): void {
    const instrument = data.instrument;

    if(!this._ticksCollectedSinceLastUpdate[instrument]) {
    this._ticksCollectedSinceLastUpdate[instrument] = {
        count: 0,
        volume: 0,
        lastPrice: data.close,
        lastTimestamp: new Date()
    };
}

// Update tick counter and volume
this._ticksCollectedSinceLastUpdate[instrument].count++;
this._ticksCollectedSinceLastUpdate[instrument].volume += data.volume;
this._ticksCollectedSinceLastUpdate[instrument].lastPrice = data.close;
this._ticksCollectedSinceLastUpdate[instrument].lastTimestamp = new Date();
}
// Continued from VPAStrategy Block #14
/**
* Update bar under construction with new tick data
* @param data Tick update data
*/
private _updateBarUnderConstruction(data: MarketData): void {
    const instrument = data.instrument;
    const timeframe = data.timeframe;
    // Skip if dynamic bar reconstruction is disabled
    if(!this._vpaParams.realTimeSettings.dynamicBarReconstruction) {
    return;
}

// Initialize timeframe structure if needed
if (!this._barsUnderConstruction[timeframe]) {
    this._barsUnderConstruction[timeframe] = {};
}

// Get the bar under construction
let bar = this._barsUnderConstruction[timeframe][instrument];

// Calculate expected bar open and close times based on timeframe
const barTimes = this._calculateBarTimes(data.timestamp, timeframe);

// If no bar exists, or the current bar is from a different time period, create a new one
if (!bar || bar.openTimestamp.getTime() !== barTimes.open.getTime()) {
    // Create a new bar
    bar = {
        instrument: instrument,
        timeframe: timeframe,
        openTimestamp: barTimes.open,
        expectedCloseTimestamp: barTimes.close,
        open: data.close, // First tick becomes the open
        high: data.close,
        low: data.close,
        current: data.close,
        volume: data.volume,
        updateCount: 1,
        lastUpdateTime: new Date(),
        analyzed: false
    };
} else {
    // Update existing bar
    bar.high = Math.max(bar.high, data.close);
    bar.low = Math.min(bar.low, data.close);
    bar.current = data.close;
    bar.volume += data.volume;
    bar.updateCount++;
    bar.lastUpdateTime = new Date();
    bar.analyzed = false; // Mark as needing analysis
}

// Store updated bar
this._barsUnderConstruction[timeframe][instrument] = bar;
}

/**
 * Mark a bar under construction as complete
 * @param data Complete bar data
 */
private _completeBarUnderConstruction(data: MarketData): void {
    const instrument = data.instrument;
    const timeframe = data.timeframe;

    // Skip if structure doesn't exist
    if(!this._barsUnderConstruction[timeframe] ||
        !this._barsUnderConstruction[timeframe][instrument]) {
    return;
}

// Reset bar under construction
this._barsUnderConstruction[timeframe][instrument] = null;

// Reset tick collection for this instrument
if (this._ticksCollectedSinceLastUpdate[instrument]) {
    this._ticksCollectedSinceLastUpdate[instrument] = {
        count: 0,
        volume: 0,
        lastPrice: data.close,
        lastTimestamp: data.timestamp
    };
}
}

/**
 * Process updates to order book data if available
 * @param instrument Instrument to update
 * @param bids Bid levels
 * @param asks Ask levels
 */
public updateOrderBookData(
    instrument: string,
    bids: { price: number; volume: number }[],
    asks: { price: number; volume: number }[]
): void {
    // Skip if order book integration is disabled
    if(!this._vpaParams?.realTimeSettings?.useOrderBookData) {
    return;
}

// Update order book data for the instrument
this._orderBookData[instrument] = {
    bids,
    asks,
    lastUpdateTime: new Date()
};
}

/**
 * Calculate the expected open and close times for a bar based on timeframe
 * @param timestamp Current time
 * @param timeframe Timeframe string (e.g., "1m", "5m", "1h")
 */
private _calculateBarTimes(timestamp: Date, timeframe: string): { open: Date, close: Date } {
    const time = new Date(timestamp);

    // Extract the number and unit from timeframe
    const match = timeframe.match(/^(\d+)([mhd])$/i);
    if (!match) {
        // Default to 1-minute bars if format is unrecognized
        const minutes = Math.floor(time.getMinutes() / 1) * 1;
        const open = new Date(time);
        open.setMinutes(minutes, 0, 0);

        const close = new Date(open);
        close.setMinutes(open.getMinutes() + 1);

        return { open, close };
    }

    const value = parseInt(match[1], 10);
    const unit = match[2].toLowerCase();

    switch (unit) {
        case 'm': // Minutes
            const minutes = Math.floor(time.getMinutes() / value) * value;
            const open = new Date(time);
            open.setMinutes(minutes, 0, 0);

            const close = new Date(open);
            close.setMinutes(open.getMinutes() + value);

            return { open, close };

        case 'h': // Hours
            const hours = Math.floor(time.getHours() / value) * value;
            const openHour = new Date(time);
            openHour.setHours(hours, 0, 0, 0);

            const closeHour = new Date(openHour);
            closeHour.setHours(openHour.getHours() + value);

            return { open: openHour, close: closeHour };

        case 'd': // Days
            const openDay = new Date(time);
            openDay.setHours(0, 0, 0, 0);

            const closeDay = new Date(openDay);
            closeDay.setDate(openDay.getDate() + value);

            return { open: openDay, close: closeDay };

        default:
            // Should never get here due to regex, but fallback to 1-minute
            const fallbackOpen = new Date(time);
            fallbackOpen.setMinutes(Math.floor(fallbackOpen.getMinutes()), 0, 0);

            const fallbackClose = new Date(fallbackOpen);
            fallbackClose.setMinutes(fallbackOpen.getMinutes() + 1);

            return { open: fallbackOpen, close: fallbackClose };
    }
}
**
* Determine if we should evaluate signals for an instrument
    * @param instrument Instrument to check
        * @param forTickUpdate Whether this is for a tick update(more restrictive)
            * /
private _shouldEvaluateSignals(instrument: string, forTickUpdate = false): boolean {
    if (!this._vpaParams || !this._lastSignalEvaluationTime[instrument]) {
        return false;
    }
    const now = new Date();
    const lastEvalTime = this._lastSignalEvaluationTime[instrument];
    const interval = this._vpaParams.realTimeSettings.signalEvaluationInterval;

    // For tick updates, use a longer interval to avoid excessive signal generation
    const requiredInterval = forTickUpdate ? interval * 2 : interval;

    // Check if enough time has passed since last evaluation
    return (now.getTime() - lastEvalTime.getTime()) >= requiredInterval;
}

/**
 * Update processing time metrics for performance tracking
 * @param processingTimeMs Processing time in milliseconds
 */
private _updateProcessingTimeMetrics(processingTimeMs: number): void {
    // Initialize if needed
    if(this._realTimeState.tickProcessingTimeSamples === 0) {
    this._realTimeState.averageTickProcessingTimeMs = processingTimeMs;
    this._realTimeState.tickProcessingTimeSamples = 1;
    return;
}

// Calculate exponential moving average (EMA) with 0.1 alpha
// This gives more weight to recent processing times
const alpha = 0.1;
this._realTimeState.averageTickProcessingTimeMs =
    (alpha * processingTimeMs) +
    ((1 - alpha) * this._realTimeState.averageTickProcessingTimeMs);

this._realTimeState.tickProcessingTimeSamples++;
}

/**
 * Get market data for analysis
 * @param instrument Instrument to get data for
 * @param timeframe Timeframe to get data for
 */
private _getDataForAnalysis(instrument: string, timeframe: string): MarketData[] {
    // If it's the primary timeframe, just return the data
    if (!this._vpaParams?.useMultiTimeframe || timeframe === this._vpaParams.primaryTimeframe) {
        return (this._marketDataCache[timeframe]?.[instrument] || []).filter(d => d.isComplete);
    }

    // For other timeframes, we need to implement multi-timeframe analysis here
    // This is a placeholder - in a real implementation, we would correlate data across timeframes
    return (this._marketDataCache[timeframe]?.[instrument] || []).filter(d => d.isComplete);
}

/**
 * Get active positions for an instrument
 * @param instrument Instrument to get positions for
 */
private _getActivePositions(instrument: string): StrategySignal[] {
    const positions: StrategySignal[] = [];

    // Find all active entry signals for this instrument
    for (const [id, signal] of this._activeSignals) {
        if (
            signal.instrument === instrument &&
            signal.type === SignalType.ENTRY &&
            !id.startsWith('exit-') &&
            !id.startsWith('trade-')
        ) {
            positions.push(signal);
        }
    }

    return positions;
}

/**
 * Calculate position size based on risk management settings
 * @param instrument Instrument to trade
 * @param direction Trade direction
 * @param entryPrice Entry price
 * @param stopLoss Stop loss price
 */
private _calculatePositionSize(
    instrument: string,
    direction: SignalDirection,
    entryPrice: number,
    stopLoss: number | null
): number {
    if (!this._config || !stopLoss) {
        return 0;
    }

    const riskSettings = this._config.riskManagement;

    // Fixed position size
    if (riskSettings.positionSizingMethod === 'fixed') {
        return Math.min(riskSettings.positionSizingValue, this._config.maxPositionSize);
    }

    // Risk percentage-based position sizing
    if (riskSettings.positionSizingMethod === 'risk-percent') {
        // Calculate risk per contract
        const riskPerContract = direction === SignalDirection.LONG ?
            entryPrice - stopLoss :
            stopLoss - entryPrice;

        if (riskPerContract <= 0) {
            return 0;
        }

        // Assume a fixed account size for now (in a real implementation, this would come from account state)
        const accountSize = 50000; // $50,000 account

        // Calculate risk amount
        const riskAmount = accountSize * (riskSettings.positionSizingValue / 100);

        // Calculate position size
        const positionSize = Math.floor(riskAmount / riskPerContract);

        return Math.min(positionSize, this._config.maxPositionSize);
    }

    // Volatility-based position sizing
    if (riskSettings.positionSizingMethod === 'volatility') {
        // In a real implementation, this would be based on ATR or other volatility measure
        // For now, just return a fixed size
        return Math.min(1, this._config.maxPositionSize);
    }

    return 0;
}

/**
 * Calculate profit from a trade
 * @param direction Trade direction
 * @param entryPrice Entry price
 * @param exitPrice Exit price
 * @param quantity Trade quantity
 */
private _calculateProfit(
    direction: SignalDirection,
    entryPrice: number,
    exitPrice: number,
    quantity: number
): number {
    const priceDiff = direction === SignalDirection.LONG ?
        exitPrice - entryPrice :
        entryPrice - exitPrice;

    // In a real implementation, this would account for instrument specifics (tick value, etc.)
    // For NQ, each point is worth $20
    return priceDiff * quantity * 20;
}
/**
* Calculate current profit for a position
* @param direction Position direction
* @param entryPrice Entry price
* @param currentPrice Current price
* @param quantity Position quantity
*/
private _calculateCurrentProfit(
    direction: SignalDirection,
    entryPrice: number,
    currentPrice: number,
    quantity: number
): number {
    const priceDiff = direction === SignalDirection.LONG ?
        currentPrice - entryPrice :
        entryPrice - currentPrice;
    // For NQ, each point is worth $20
    return priceDiff * quantity * 20;
}

/**
 * Calculate maximum drawdown from trade history
 * @param trades Trade history
 */
private _calculateMaxDrawdown(trades: Trade[]): number {
    if (trades.length === 0) {
        return 0;
    }

    // Sort trades by exit time
    const sortedTrades = [...trades].filter(t => t.exitTime !== undefined)
        .sort((a, b) => (a.exitTime as Date).getTime() - (b.exitTime as Date).getTime());

    let maxBalance = 0;
    let maxDrawdown = 0;
    let runningBalance = 0;

    for (const trade of sortedTrades) {
        runningBalance += trade.profit || 0;

        if (runningBalance > maxBalance) {
            maxBalance = runningBalance;
        }

        const drawdown = maxBalance - runningBalance;
        if (drawdown > maxDrawdown) {
            maxDrawdown = drawdown;
        }
    }

    return maxDrawdown;
}

/**
 * Calculate maximum consecutive wins or losses
 * @param trades Trade history
 * @param countWins Whether to count wins (true) or losses (false)
 */
private _calculateMaxConsecutive(trades: Trade[], countWins: boolean): number {
    if (trades.length === 0) {
        return 0;
    }

    let maxConsecutive = 0;
    let currentConsecutive = 0;

    for (const trade of trades) {
        const isWin = (trade.profit || 0) > 0;

        if ((countWins && isWin) || (!countWins && !isWin)) {
            currentConsecutive++;
            maxConsecutive = Math.max(maxConsecutive, currentConsecutive);
        } else {
            currentConsecutive = 0;
        }
    }

    return maxConsecutive;
}

/**
 * Get all timeframes used by the strategy
 */
private _getTimeframes(): string[] {
    if (!this._vpaParams || !this._vpaParams.useMultiTimeframe) {
        return [this._vpaParams?.primaryTimeframe || '5m'];
    }

    return [
        this._vpaParams.primaryTimeframe,
        this._vpaParams.trendTimeframe,
        this._vpaParams.entryTimeframe
    ].filter(Boolean) as string[];
}

/**
 * Perform Volume Price Analysis on the given data
 * @param data Market data to analyze
 */
private _performVPAAnalysis(data: MarketData[]): VPAAnalysisResult {
    // Comprehensive VPA analysis implementation
    // For brevity, the implementation details are omitted here
    // This method would analyze price patterns, volume characteristics, 
    // trend conditions, support/resistance levels, etc.

    // This is a placeholder returning a default structure
    // In a real implementation, this would be a sophisticated analysis
    return {
        direction: SignalDirection.FLAT,
        confidence: 0,
        volumeScore: 0,
        priceScore: 0,
        patternScore: 0,
        trendScore: 0,
        srScore: 0,
        patterns: [],
        trend: null,
        supportResistance: null,
        breakout: {
            detected: false,
            type: null,
            level: null,
            strength: 'weak',
            confirmed: false
        },
        potentialReversal: {
            detected: false,
            confidence: 0,
            direction: null
        },
        targetPrice: null,
        stopLoss: null
    };
}
